from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import window,col,count,max,when, min

spark=SparkSession.builder.appName("streaming_sessions").getOrCreate()

schema = StructType([
StructField("user_id", StringType(), True),
StructField("event_time", TimestampType(), True),
StructField("event_type", StringType(), True),
])

def func():

    df=spark.readStream.format("json").schema(schema).load("path")

    df=df.withWatermark("event_time","1 hour")

    agg_df=df.groupBy("user_id",window(col("event_time"),"30 minutes")).agg(
        count("*").alias("event_count"),
        (max("event_time").cast("long")-min("event_time").cast("long")).alias("session_duration"),
        max(when(col("event_type")=='purchase',1).otherwise(0)).alias("had_purchase_tmp")
    )
    summary_df=agg_df.withColumn("had_purchase",when(col("had_purchase_tmp")>=1, 1).otherwise(0))

    query = (
    summary_df.writeStream
    .format("parquet")
    .option("path", "S3_OUTPUT_PATH")
    .option("checkpointLocation", "CHECKPOINT_LOCATION")
    .outputMode("append")
    .start()
    )


    query.awaitTermination()

func()