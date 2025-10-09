from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum, when, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ---------------------------
# Spark session
# ---------------------------
spark = SparkSession.builder \
    .appName("OrderAnalyticsPipelineFullRead") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ---------------------------
# Paths
# ---------------------------
INPUT_PATH = "s3://company-orders/stream/"
ORDERS_DELTA_PATH = "s3://company-analytics/orders_state/"
SUMMARY_DELTA_PATH = "s3://company-analytics/daily_order_summary/"
CHECKPOINT_PATH = "s3://company-analytics/checkpoints/daily_order_summary/"

# ---------------------------
# Schema
# ---------------------------
orders_schema = """
    order_id STRING,
    user_id STRING,
    order_time TIMESTAMP,
    order_amount DOUBLE,
    status STRING
"""

# ---------------------------
# Read streaming data
# ---------------------------
raw_orders_stream = spark.readStream \
    .format("json") \
    .schema(orders_schema) \
    .option("maxFilesPerTrigger", 100) \
    .load(INPUT_PATH)

orders_with_watermark = raw_orders_stream.withWatermark("order_time", "3 days") \
                                         .withColumn("order_date", to_date(col("order_time")))

# ---------------------------
# Micro-batch processing function
# ---------------------------
def process_micro_batch(batch_df, batch_id):
    
    # ---------------------------
    # Deduplicate within batch to pick latest order_time per order_id
    # ---------------------------
    window_spec = Window.partitionBy("order_id").orderBy(col("order_time").desc())
    batch_df = batch_df.withColumn("rank", row_number().over(window_spec)) \
                       .filter(col("rank") == 1) \
                       .drop("rank")
    
    # ---------------------------
    # Upsert raw orders table (to keep full latest order state)
    # ---------------------------
    if DeltaTable.isDeltaTable(spark, ORDERS_DELTA_PATH):
        orders_table = DeltaTable.forPath(spark, ORDERS_DELTA_PATH)
        orders_table.alias("t").merge(
            batch_df.alias("s"),
            "t.order_id = s.order_id"
        ).whenMatchedUpdate(
            condition="s.order_time > t.order_time",
            set={
                "user_id": col("s.user_id"),
                "order_time": col("s.order_time"),
                "order_amount": col("s.order_amount"),
                "status": col("s.status"),
                "order_date": col("s.order_date")
            }
        ).whenNotMatchedInsertAll().execute()
    else:
        batch_df.write.format("delta").mode("overwrite").save(ORDERS_DELTA_PATH)
    
    # ---------------------------
    # Full table read to compute cumulative aggregates
    # ---------------------------
    orders_current = spark.read.format("delta").load(ORDERS_DELTA_PATH)
    
    daily_agg = orders_current.groupBy("order_date").agg(
        count("order_id").alias("total_orders"),
        sum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
        sum(when(col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_orders"),
        sum(when(col("status") == "DELIVERED", col("order_amount")).otherwise(0)).alias("total_revenue")
    )
    
    # ---------------------------
    # Merge aggregates into summary Delta table
    # ---------------------------
    if DeltaTable.isDeltaTable(spark, SUMMARY_DELTA_PATH):
        summary_table = DeltaTable.forPath(spark, SUMMARY_DELTA_PATH)
        summary_table.alias("t").merge(
            daily_agg.alias("s"),
            "t.order_date = s.order_date"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        daily_agg.write.format("delta").mode("overwrite").save(SUMMARY_DELTA_PATH)

# ---------------------------
# Start streaming query
# ---------------------------
streaming_query = orders_with_watermark.writeStream \
    .foreachBatch(process_micro_batch) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="5 minutes") \
    .start()

streaming_query.awaitTermination()
