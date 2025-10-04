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

#define state
class UserState(NamedTuple):
    total_events: int
    last_event_time: datetime
    has_purchased: bool


def update_state(user_id, rows_iterator, state: GroupState):

    if state.exists():
        cur_state=state.get()
    else:
        cur_state=UserState(total_events=0,last_event_time=None,has_purchased=False)
    
    ct=cur_state.total_events
    last_event_time=cur_state.last_event_time
    has_p=cur_state.has_purchased

    for each in rows_iterator:
        ct+=1

        if last_event_time is None or each.event_time>last_event_time:
            last_event_time=each.event_time
        
        if each.event_type =='purchase':
            has_p=True
    
    new_state=UserState(total_events=ct, last_event_time=last_event_time,has_purchased=has_p)
    state.update(new_state)

    return (user_id, ct, last_event_time, has_p)


df=spark.readStream.format("json").schema(schema).load("s3_path")

df=df.withWatermark("event_time","1 hour")
tuples=df.groupByKey(lambda row:row.user_id).mapGroupsWithState(
    func=update_state,
    output_mode="update",
    stateTimeout=GroupStateTimeout.NoTimeout # this makes the state persistent, unbounded timelimit
)

#conver the tuples to df
df = tuples.toDF(["user_id", "total_events", "last_event_time", "has_purchased"])

query=df.writeStream.outputMode("update").format("parquet").option("path","s3_path").option("checkpointLocation","s3_checkpoint_path").start()
query.awaitTermination()