from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from delta.tables import DeltaTable
from typing import Iterator, NamedTuple
import uuid
import datetime

# =============================================
# Initialize Spark with Delta Lake
# =============================================
spark = (
    SparkSession.builder
    .appName("Delta Stateful Streaming Deduplication")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# =============================================
# Input schema for events
# =============================================
input_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("ts_ingested", TimestampType(), True)
])

# Output schema after deduplication
output_schema = StructType([
    StructField("version_id", StringType(), False),
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("ts_ingested", TimestampType(), True),
    StructField("operation", StringType(), True)
])


class EventState(NamedTuple):
    last_event_time: datetime.datetime
    version_id: str

def deduplicate_with_state(
    event_id: str,
    events: Iterator[Row],
    state: GroupState
) -> Iterator[Row]:

    WINDOW_HOURS = 24
    results = []
    latest_event = None

    # Pick latest event in this micro-batch
    for event in events:
        if latest_event is None or event.event_time > latest_event.event_time:
            latest_event = event

    # Retrieve previous state
    if state.exists:
        last_state = state.get()
        last_ts = last_state.last_event_time
        last_version_id = last_state.version_id
    else:
        last_ts = None
        last_version_id = None

    if latest_event:
        if last_ts is None:
            # First occurrence -> INSERT
            version_id = str(uuid.uuid4())
            results.append(Row(
                version_id=version_id,
                event_id=latest_event.event_id,
                user_id=latest_event.user_id,
                url=latest_event.url,
                event_time=latest_event.event_time,
                device=latest_event.device,
                browser=latest_event.browser,
                ts_ingested=latest_event.ts_ingested,
                operation="INSERT"
            ))
            state.update(EventState(latest_event.event_time, version_id))
        else:
            # Time difference in hours
            time_diff_hrs = (latest_event.event_time - last_ts).total_seconds() / 3600
            if time_diff_hrs <= WINDOW_HOURS:
                # Within window -> soft delete old + update new version
                version_id = last_version_id
                # Delete old
                results.append(Row(
                    version_id=version_id,
                    event_id=latest_event.event_id,
                    user_id=latest_event.user_id,
                    url=latest_event.url,
                    event_time=latest_event.event_time,
                    device=latest_event.device,
                    browser=latest_event.browser,
                    ts_ingested=latest_event.ts_ingested,
                    operation="DELETE"
                ))
                # Insert new version
                results.append(Row(
                    version_id=version_id,
                    event_id=latest_event.event_id,
                    user_id=latest_event.user_id,
                    url=latest_event.url,
                    event_time=latest_event.event_time,
                    device=latest_event.device,
                    browser=latest_event.browser,
                    ts_ingested=latest_event.ts_ingested,
                    operation="UPDATE"
                ))
                state.update(EventState(latest_event.event_time, version_id))
            else:
                # New version
                version_id = str(uuid.uuid4())
                results.append(Row(
                    version_id=version_id,
                    event_id=latest_event.event_id,
                    user_id=latest_event.user_id,
                    url=latest_event.url,
                    event_time=latest_event.event_time,
                    device=latest_event.device,
                    browser=latest_event.browser,
                    ts_ingested=latest_event.ts_ingested,
                    operation="NEW_VERSION"
                ))
                state.update(EventState(latest_event.event_time, version_id))

    # Set timeout for 25 hours
    state.setTimeoutDuration(25 * 60 * 60 * 1000)
    return iter(results)


raw_stream = (
    spark.readStream
    .format("json")
    .schema(input_schema)
    .option("maxFilesPerTrigger", 50)
    .load("s3://raw/clicks/")
)

watermarked = raw_stream.withWatermark("event_time", "25 hours")

# =============================================
# Apply stateful deduplication
# =============================================
deduplicated = (
    watermarked
    .groupByKey(lambda row: row.event_id)
    .mapGroupsWithState(
        func=deduplicate_with_state,
        outputType=output_schema,
        stateTimeout=GroupStateTimeout.ProcessingTimeTimeout
    )
)

# =============================================
# ForeachBatch Delta upsert
# =============================================
def upsert_to_delta(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    delta_path = "s3://delta/clicks_cleaned/"

    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
    except:
        # First run -> create table
        batch_df.drop("operation").write.format("delta").mode("overwrite").save(delta_path)
        print(f"✅ Batch {batch_id}: Created Delta table")
        return

    # Separate DELETE and INSERT/UPDATE/NEW_VERSION
    deletes = batch_df.filter(col("operation") == "DELETE")
    inserts = batch_df.filter(col("operation").isin(["INSERT", "UPDATE", "NEW_VERSION"]))

    if not deletes.isEmpty():
        delta_table.alias("target").merge(
            deletes.select("version_id").distinct().alias("source"),
            "target.version_id = source.version_id"
        ).whenMatchedDelete().execute()

    if not inserts.isEmpty():
        inserts.drop("operation").write.format("delta").mode("append").save(delta_path)

    print(f"✅ Batch {batch_id} completed")

# =============================================
# Start streaming query
# =============================================
query = (
    deduplicated
    .writeStream
    .foreachBatch(upsert_to_delta)
    .outputMode("update")
    .option("checkpointLocation", "s3://checkpoints/clicks_dedup/")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
