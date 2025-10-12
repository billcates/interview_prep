from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from delta.tables import DeltaTable
from typing import NamedTuple, Iterator
import uuid

# ================================
# Initialize Spark
# ================================
spark = (
    SparkSession.builder
    .appName("Delta Stateful Deduplication")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ================================
# Input Schema
# ================================
input_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("ts_ingested", TimestampType(), True)
])

# ================================
# Output Schema
# ================================
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

# ================================
# State Definition
# ================================
class EventState(NamedTuple):
    last_event_time: TimestampType
    version_id: str

# ================================
# Stateful Deduplication Function
# ================================
def deduplicate_with_state(
    event_id: str,
    events: Iterator[Row],
    state: GroupState[EventState]
) -> Iterator[Row]:
    """
    Stateful deduplication logic:
    - If event seen within 24hrs → mark old DELETE, new UPDATE (same version)
    - If event after 24hrs → NEW_VERSION
    """
    WINDOW_HOURS = 24
    results = []

    # Sort events for this key to pick the latest
    latest_event = max(events, key=lambda x: x.event_time)

    # Check existing state
    if state.exists:
        prev_state = state.get
        last_ts = prev_state.last_event_time
        version_id = prev_state.version_id
    else:
        last_ts = None
        version_id = None

    if last_ts is None:
        # First time seeing this event
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
    else:
        # Compare timestamps
        diff_hours = (latest_event.event_time - last_ts).total_seconds() / 3600
        if diff_hours <= WINDOW_HOURS:
            # Within 24hr window → mark old DELETE + new UPDATE
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
        else:
            # New version after 24hrs
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

    # Update state
    state.update(EventState(last_event_time=latest_event.event_time, version_id=version_id))
    state.setTimeoutDuration(25 * 3600 * 1000)  # 25 hours in ms

    return iter(results)

# ================================
# Read Streaming Data
# ================================
raw_stream = (
    spark.readStream
    .format("json")
    .schema(input_schema)
    .option("maxFilesPerTrigger", 50)
    .load("s3://raw/clicks/")
)

watermarked = raw_stream.withWatermark("event_time", "25 hours")

# ================================
# Apply flatMapGroupsWithState
# ================================
deduplicated = (
    watermarked
    .groupByKey(lambda row: row.event_id)
    .flatMapGroupsWithState(
        func=deduplicate_with_state,
        outputMode="append",
        stateTimeout=GroupStateTimeout.ProcessingTimeTimeout
    )
)

# ================================
# ForeachBatch → Delta Upsert
# ================================
def upsert_to_delta(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    delta_path = "s3://delta/clicks_cleaned/"

    try:
        delta_table = DeltaTable.forPath(spark, delta_path)
    except:
        batch_df.drop("operation").write.format("delta").mode("overwrite").save(delta_path)
        print(f"✅ Batch {batch_id}: Created Delta table")
        return

    deletes = batch_df.filter(col("operation") == "DELETE")
    inserts = batch_df.filter(col("operation").isin(["INSERT", "UPDATE", "NEW_VERSION"]))

    if not deletes.isEmpty():
        delta_table.alias("target").merge(
            deletes.select("version_id").distinct().alias("source"),
            "target.version_id = source.version_id"
        ).whenMatchedDelete().execute()
        print(f"🗑️  Deleted {deletes.count()} old versions")

    if not inserts.isEmpty():
        inserts.drop("operation").write.format("delta").mode("append").save(delta_path)
        print(f"✅ Inserted {inserts.count()} records")

    print(f"✅ Batch {batch_id} complete")

# ================================
# Start Streaming Query
# ================================
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
