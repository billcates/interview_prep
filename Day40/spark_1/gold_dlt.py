from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp,col,sum


@dp.table(
        name="sales_gold",
        comment="Gold Table"
)
def load_gold_table():
    df = dp.read_stream("sales_silver").withWatermark("sale_date","1 day")
    df=(df.groupBy("category","sale_date").agg(
        sum(col("total_amount")).alias("total_sale_amount"),
        sum(col("quantity")).alias("total_quantity"))
        .withColumn("last_updated_time",current_timestamp()))
    return df

# dlt can't handle updates to the delta table, it can append only
#though we have create_auto_cd.c_flow it can be used only for tracking row level changes, not streaming aggregates