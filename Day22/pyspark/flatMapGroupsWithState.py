from typing import NamedTuple
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import GroupState, GroupStateTimeout

spark=SparkSession.builder.appName("mapGroupWithState").getOrCreate()

schema = StructType([
StructField("user_id", StringType(), True),
StructField("event_time", TimestampType(), True),
StructField("event_type", StringType(), True),
])

class Session(NamedTuple):
    session_id : str
    start_time : datetime
    last_event_time : datetime
    event_count : int
    has_purchase : bool

SESSION_TIMEOUT_MINUTES=30

def start_new_session(event) -> Session:
    return Session(
        session_id=event.event_time.strftime("%Y%m%d%H%M%S"),
        start_time=event.event_time,
        last_event_time=event.event_time,
        event_count=1,
        has_purchase=event.event_type == "purchase"
    )

def update_session(session: Session, event) -> Session:
    return Session(
        session_id=session.session_id,
        start_time=session.start_time,
        last_event_time=event.event_time,
        event_count=session.event_count + 1,
        has_purchase=session.has_purchase or event.event_type == "purchase"
    )



def update_session_fun(user_id,rows_iterator,state):
    events = sorted(list(rows_iterator), key=lambda e: e.event_time)
    if state.exists():
        session=state.get()
    else :
        session = None

    outputs=[]

    for event in events:
        if session is None:
            session = start_new_session(event)
        else:
            gap = (event.event_time - session.last_event_time).total_seconds() / 60
            if gap > SESSION_TIMEOUT_MINUTES:
                # Session ended → emit
                outputs.append((
                    user_id,
                    session.session_id,
                    session.start_time,
                    session.last_event_time,
                    session.event_count,
                    session.has_purchase
                ))
                session = start_new_session(event)
            else:
                session = update_session(session, event)

    state.update(session)


    state.setTimeoutTimestamp(int(session.last_event_time.timestamp() * 1000) + SESSION_TIMEOUT_MINUTES * 60 * 1000)
    if state.hasTimedOut:
        outputs.append((
            user_id,
            session.session_id,
            session.start_time,
            session.last_event_time,
            session.event_count,
            session.has_purchase
        ))
        state.remove()
    
    return iter(outputs)

df = spark.readStream.format("json").schema(schema).load("s3_path")

df=df.withWatermark("event_time","1 hour")

res=df.groupByKey(lambda row:row.user_id).flatMapGroupsWithState(
    func=update_session_fun,
    output_mode="append",
    timeout=GroupStateTimeout.EventTimeTimeout
)

res_df=res.toDF(["user_id","session_id","start_time","end_time","event_count","has_purchase"])

query=res_df.writeStream.outputMode("update").format("parquet").option("path","s3_path").option("checkpointLocation","s3_checkpoint_path").start()
query.awaitTermination()