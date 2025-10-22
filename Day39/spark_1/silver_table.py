from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, input_file_name,col

valid_records = {"valid_prize": "prize > 0", "valid_quantity": "quantity >=1", "valid_product": "product_id is not NULL"}


@dp.table(
    name="sales_bronze",
    comment="Raw sales data ingested incrementally from CSV files using Auto Loader"
)
def load_sales_bronze():
    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .load("/mnt/raw/sales/")
          .withColumn("load_time", current_timestamp())
          .withColumn("source_file", input_file_name()))
    return df

@dp.table(
        name="sales_silver",
        comment="Silver Table after dropping invalid records and some transformations"
)
@dp.expect_or_drop(valid_records)
def load_silver_table():
    df=spark.read.table("sales_bronze")
    df=df.withColumn("total_amount",  col("price").cast("double") * col("quantity").cast("double"))
    return df