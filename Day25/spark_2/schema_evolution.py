from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql import functions as f

from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("delta_table_load") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.DeltaCatalog")\
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

def process_func(batch_df, batch_id):
    delta_path = "s3://company-analytics/daily_txn_summary/"
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        main_tbl = DeltaTable.forPath(spark, delta_path)
        
        main_tbl.alias("t").merge(
            batch_df.alias("s"),
            "t.txn_date = s.txn_date AND t.currency = s.currency"
        ).whenMatchedUpdate(set={
            "total_transactions":  f.col("s.total_transactions"),
            "unique_users": f.col("s.unique_users"), 
            "total_amount": f.col("s.total_amount") 
        }).whenNotMatchedInsertAll().execute()
    else:
        batch_df.write.format("delta").mode("append").save(delta_path)


# Read streaming data
df = spark.readStream.format("json").option("mergeSchema", "true").load("s3://company-data/user_events/")

all_columns = df.columns
core_columns = ["txn_id", "user_id", "txn_time", "amount", "currency"]

select_list = [
    f.col("txn_id").cast("string"),
    f.col("user_id").cast("string"),
    f.col("txn_time").cast("timestamp"),
    f.col("amount").cast("double"),
    f.col("currency").cast("string")
]

extra_columns = [f.col(c) for c in all_columns if c not in core_columns]
select_list.extend(extra_columns)

df = df.select(*select_list)

df = df.withWatermark("txn_time", "2 days")

df = df.withColumn("txn_date", f.to_date(f.col("txn_time")))


agg_df = df.groupBy("txn_date", "currency").agg(
    f.count("txn_id").alias("total_transactions"),
    f.countDistinct("user_id").alias("unique_users"), 
    f.sum("amount").alias("total_amount") 
)

# Start streaming query
query = (agg_df.writeStream
    .foreachBatch(process_func)
    .outputMode("update")
    .option("checkpointLocation", "s3://company-analytics/checkpoints/daily_txn_summary/")
    .start())

query.awaitTermination()