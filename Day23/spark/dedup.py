from typing import NamedTuple
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import GroupState, GroupStateTimeout

spark = SparkSession.builder.appName("mapGroupWithState").getOrCreate()

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
])

# Define state
class UserState(NamedTuple):
    event_ids: list


def update_state(user_id, rows_iterator, state: GroupState):

    timeout_duration = 60 * 60 * 1000  # 1 hour in milliseconds
    
    if state.hasTimedOut:
        state.remove()
        return iter([])

    if state.exists():
        cur_state = state.get()
        event_ids = list(cur_state.event_ids)
    else:
        event_ids = []
    
    result = []
    
    for row in rows_iterator:
        if row.event_id not in event_ids:
            event_ids.append(row.event_id)
            result.append(Row(
                user_id=row.user_id,
                event_id=row.event_id,
                event_time=row.event_time,
                event_type=row.event_type
            ))
    
    new_state = UserState(event_ids=event_ids)
    state.update(new_state)
    state.setTimeoutDuration(timeout_duration)
    
    return iter(result)


# Read stream
df = spark.readStream.format("json").schema(schema).load("s3_path")

df = df.withWatermark("event_time", "1 hour")

deduplicated_df = df.groupByKey(lambda row: row.user_id).mapGroupsWithState(
    func=update_state,
    outputMode="update",
    stateTimeout=GroupStateTimeout.ProcessingTimeTimeout
)

query = deduplicated_df.writeStream \
    .outputMode("update") \
    .format("parquet") \
    .option("path", "s3_output_path") \
    .option("checkpointLocation", "s3_checkpoint_path") \
    .start()

query.awaitTermination()