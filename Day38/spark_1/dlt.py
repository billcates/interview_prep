import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

@dlt.table(
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
