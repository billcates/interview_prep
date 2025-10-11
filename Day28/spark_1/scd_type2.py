from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark with Delta configs
spark = SparkSession.builder \
    .appName("SCD_Type2_Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Paths
TABLE_PATH = "s3://company-analytics/users_scd2/"
CHECKPOINT_PATH = "s3://company-analytics/checkpoints/users_scd2/"

# Schema for source stream
schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("updated_at", TimestampType(), False)
])

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty, skipping.")
        return

    # Prepare incoming dataframe in SCD2 format
    incoming_df = batch_df.select(
        "user_id",
        "name",
        "city",
        col("updated_at").alias("valid_from"),
        lit(None).cast("timestamp").alias("valid_to"),
        lit(1).alias("is_active")
    )

    # First batch → Initialize Delta Table
    if not DeltaTable.isDeltaTable(spark, TABLE_PATH):
        print(f"Batch {batch_id}: Creating initial SCD2 table")
        incoming_df.write.format("delta").mode("overwrite").save(TABLE_PATH)
        return

    # Load table reference
    delta_table = DeltaTable.forPath(spark, TABLE_PATH)

    # ------------------------------------------------------
    # ✅ PHASE 1 — CLOSE EXISTING ACTIVE ROWS WHERE CHANGE DETECTED
    # ------------------------------------------------------
    delta_table.alias("t").merge(
        incoming_df.alias("s"),
        "t.user_id = s.user_id AND t.is_active = 1"
    ).whenMatchedUpdate(
        condition=""" 
            (s.name <> t.name OR s.city <> t.city)
            OR (s.name IS NULL AND t.name IS NOT NULL)
            OR (s.name IS NOT NULL AND t.name IS NULL)
            OR (s.city IS NULL AND t.city IS NOT NULL)
            OR (s.city IS NOT NULL AND t.city IS NULL)
        """,
        set={
            "is_active": "0",
            "valid_to": "s.valid_from"
        }
    ).whenNotMatchedInsertAll().execute()

    # ------------------------------------------------------
    # ✅ PHASE 2 — INSERT NEW ACTIVE RECORDS FOR UPDATED USERS
    # ------------------------------------------------------
    # Select only those records that were closed in Phase 1
    delta_after_merge = delta_table.toDF()

    new_versions_df = delta_after_merge.alias("t").join(
        incoming_df.alias("s"),
        on="user_id",
        how="inner"
    ).where(
        # t.valid_to == s.valid_from AND t.is_active == 0 → means record was closed now
        (col("t.is_active") == 0) & 
        (col("t.valid_to") == col("s.valid_from"))
    ).select(
        col("s.user_id"),
        col("s.name"),
        col("s.city"),
        col("s.valid_from"),
        col("s.valid_to"),
        col("s.is_active")
    ).distinct()

    if not new_versions_df.isEmpty():
        new_versions_df.write.format("delta").mode("append").save(TABLE_PATH)
        print(f"Batch {batch_id}: Inserted {new_versions_df.count()} new active records")
    else:
        print(f"Batch {batch_id}: No new versions inserted")

# ======================================================
# 🚀 START STREAM
# ======================================================
incoming_stream = spark.readStream \
    .format("json") \
    .schema(schema) \
    .load("s3://company-data/users/")

stream = incoming_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

stream.awaitTermination()
