from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("Dynamic Partition Overwrite")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_time", TimestampType(), True)
])

def process_batch(batch_df, batch_id):
    (
        batch_df
        .withColumn("date", date_format(col("event_time"), "yyyy-MM-dd")) 
        .format("delta")
        .mode("overwrite")  
        .option("partitionOverwriteMode", "dynamic") 
        .partitionBy("date")
        .save("s3://bucket/path/delta_table/")
    )

df = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load("s3://bucket/path/input/")
)

query = (
    df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "s3://bucket/path/checkpoint/") 
    .start()
)

query.awaitTermination()
