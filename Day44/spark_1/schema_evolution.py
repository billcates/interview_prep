import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="marketing_bronze"
)
def load_bronze_data():
    df=(spark.readStream.format("cloudfiles").option("cloudFiles.format","csv").option("cloudFiles.inferColumnTypes","true")
        .option("cloudFiles.schemaEvolutionMode","addNewColumns").option("cloudFiles.schemaLocation","dbfs:/mnt/schema/marketing_campaigns/")
        .option("rescuedDataColumn","_rescued_data")
        .option("cloudFiles.schemaHints", "clicks double, impressions double, event_time TIMESTAMP")
        .load("/mnt/raw/marketing_campaigns/"))
    return df

@dlt.table(
    name="marketing_silver"
)
@dlt.expect_or_drop("valid_records","ctr <= 1")
def load_silver_table():
    df=dlt.read("marketing_bronze")
    df=df.withColumn("ctr",col("clicks")/col("impressions"))
    return df