from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,TimestampType, DoubleType
from pyspark.sql import functions as f

from delta.tables import DeltaTable

schema=StructType([
StructField("user_id",StringType(),True),
StructField("event_time",TimestampType(),True),
StructField("event_type",StringType(),True),
StructField("event_value",DoubleType(),True),
])

spark = SparkSession.builder \
    .appName("delta_table_load") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.DeltaCatalog") \
    .getOrCreate()

def process_func(batch_df,batch_id):

    if spark.catalog.tableExists("bill_testing_delta"):
        main_tbl=DeltaTable.forName(spark,"bill_testing_delta")
        main_tbl.alias("t").merge(
            batch_df.alias("s"),
            "t.event_date = s.event_date AND t.event_type = s.event_type"
            ).whenMatchedUpdate(set={
        "number_of_users": f.col("s.number_of_users"),
        "total_revenue": f.col("s.total_revenue")
        }).whenNotMatchedInsertAll().execute()
    else:
        batch_df.write.format("delta").mode("append").saveAsTable("bill_testing_delta")
    


df=spark.readStream.format("json").schema(schema).load("s3://company-data/user_events/")

df=df.withWatermark("event_time","1 day")

df=df.withColumn("event_date",f.date_trunc('day',f.col("event_time")))
agg_df=df.groupBy("event_date","event_type").agg(
    f.countDistinct("user_id").alias("number_of_users"),
    f.sum("event_value").alias("total_revenue")
)


query=(agg_df.writeStream.
    foreachBatch(process_func).
    outputMode("update").option("checkpointLocation", "s3://company-analytics/checkpoints/daily_agg/").
    start())

query.awaitTermination()