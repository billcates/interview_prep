import dlt 
from pyspark.sql.functions import current_timestamp,expr,col

schema="""
customer_id    STRING,
name           STRING,
email          STRING,
city           STRING,
operation      STRING,
event_time     TIMESTAMP
"""

@dlt.table(
    name="customers_bronze"
)
def load_bronze():
    df=spark.readStream.format("cloudfiles").schema(schema).load("/mnt/raw/customer_changes/")
    df=df.withColumn("load_time",current_timestamp()).withColumn("source_file_name",col("_metadata.file_name"))
    return df 

dlt.create_streaming_table("customers_silver")

dlt.create_auto_cdc_flows(
    source="customers_bronze",
    target="customers_silver",
    keys=["customer_id"],
    sequence_by='event_time',
    stored_as_scd_type=1,
    apply_as_deletes=expr("operation='DELETE'"),
    except_column_list=["operation"]
)