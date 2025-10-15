from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode_outer, col, to_date, sum as _sum, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, 
    IntegerType, DoubleType, TimestampType
)

spark = SparkSession.builder \
    .appName("FlattenNestedOrders") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("product_id", StringType(), True),
            StructField("qty", IntegerType(), True),
            StructField("price", DoubleType(), True)
        ])
    ), True),
    StructField("order_time", TimestampType(), True)
])

s3_input_path = "s3://your-bucket/orders/json/"

orders_stream = spark.readStream \
    .schema(order_schema) \
    .json(s3_input_path)
orders_stream = orders_stream.withWatermark("order_time", "1 day")


flattened_orders = orders_stream \
    .withColumn("item", explode_outer(col("items"))) \
    .select(
        col("order_id"),
        col("user_id"),
        col("order_time"),
        col("item.product_id").alias("product_id"),
        col("item.qty").alias("qty"),
        col("item.price").alias("price")
    )

orders_with_total = flattened_orders \
    .withColumn("total_price", col("qty") * col("price"))

orders_with_date = orders_with_total \
    .withColumn("order_date", to_date(col("order_time")))

daily_product_aggregates = orders_with_date \
    .groupBy("order_date", "product_id") \
    .agg(
        _sum("qty").alias("total_qty"),
        _sum("total_price").alias("total_revenue")
    ) \
    .orderBy("order_date", "product_id")

delta_output_path = "s3://your-bucket/delta/daily_product_stats/"
checkpoint_path = "s3://your-bucket/checkpoints/daily_product_stats/"

query = daily_product_aggregates.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("checkpointLocation", checkpoint_path) \
    .partitionBy("order_date") \
    .start(delta_output_path)

query.awaitTermination()
