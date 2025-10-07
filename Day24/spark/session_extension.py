from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from collections import namedtuple

spark = SparkSession.builder.appName("SessionExtensionMerging").getOrCreate()

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
])

SessionState = namedtuple("SessionState", [
    "session_id", "start_time", "end_time",
    "event_count", "has_purchase", "last_event_time"
])
CombinedState = namedtuple("CombinedState", ["current_session", "last_ended_session"])

SESSION_TIMEOUT_MIN = 30
MERGE_WINDOW_MIN = 10

def session_update(user_id, events_iter, state: GroupState):
    events = sorted(list(events_iter), key=lambda e: e.event_time)
    outputs = []

    current_session, last_ended_session = None, None
    if state.exists:
        st = state.get()
        current_session, last_ended_session = st.current_session, st.last_ended_session

    for event in events:
        event_time, event_type = event.event_time, event.event_type

        if not current_session:
            current_session = SessionState(1, event_time, event_time, 1, event_type == "purchase", event_time)
            continue

        gap = (event_time - current_session.last_event_time).total_seconds() / 60.0

        if gap <= SESSION_TIMEOUT_MIN:
            current_session = current_session._replace(
                end_time=event_time,
                last_event_time=event_time,
                event_count=current_session.event_count + 1,
                has_purchase=current_session.has_purchase or event_type == "purchase"
            )
        elif last_ended_session and gap <= SESSION_TIMEOUT_MIN + MERGE_WINDOW_MIN:
            # merge with previous
            current_session = current_session._replace(
                start_time=last_ended_session.start_time,
                event_count=current_session.event_count + last_ended_session.event_count,
                has_purchase=current_session.has_purchase or last_ended_session.has_purchase,
                last_event_time=event_time,
                end_time=event_time
            )
        else:
            # emit closed session
            outputs.append((
                user_id,
                f"s{current_session.session_id}",
                current_session.start_time,
                current_session.end_time,
                current_session.event_count,
                current_session.has_purchase
            ))
            last_ended_session = current_session
            current_session = SessionState(current_session.session_id + 1, event_time, event_time, 1, event_type == "purchase", event_time)

    state.update(CombinedState(current_session, last_ended_session))
    state.setTimeoutDuration(f"{SESSION_TIMEOUT_MIN} minutes")

    return outputs

# Streaming input
df = spark.readStream.format("json").schema(schema).load("s3://my-bucket/raw-events/")
df = df.withWatermark("event_time", "40 minutes")

sessionized = df.groupByKey(lambda row: row.user_id).flatMapGroupsWithState(
    outputMode="append",
    stateFunc=session_update,
    timeoutConf=GroupStateTimeout.EventTimeTimeout
)

columns = ["user_id", "session_id", "session_start", "session_end", "event_count", "has_purchase"]
session_df = sessionized.toDF(columns)

query = (session_df.writeStream
    .format("parquet")
    .option("path", "s3://my-bucket/sessionized-output/")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/session-extension/")
    .outputMode("append")
    .start())

query.awaitTermination()
