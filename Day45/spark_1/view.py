import dlt
from pyspark.sql.functions import col, current_timestamp, date_trunc, sum as _sum

@dlt.table(
    name="sales_bronze",
)
def load_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/mnt/raw/sales/")
    )
    return df.withColumn("load_time", current_timestamp())

@dlt.table(
    name="sales_silver",
    comment="Cleansed sales data with computed total amount"
)
def clean_sales():
    df = dlt.read_stream("sales_bronze")
    df = df.filter((col("quantity") > 0) & (col("price") > 0))
    return df.withColumn("total_amount", col("quantity") * col("price"))

@dlt.view(
    name="daily_sales_view",
)
def daily_sales_view():
    df = dlt.read("sales_silver")
    agg = (
        df.groupBy(
            date_trunc("day", col("order_date")).alias("order_day"),
            col("product_id")
        )
        .agg(
            _sum("total_amount").alias("daily_sales"),
            _sum("quantity").alias("daily_quantity")
        )
    )
    return agg

@dlt.table(
    name="daily_sales_gold",
    comment="Materialized daily product sales for BI and analytics"
)
def daily_sales_gold():
    df = dlt.read("daily_sales_view")
    return df.withColumn("last_updated_time", current_timestamp())