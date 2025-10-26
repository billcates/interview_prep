import dlt
from pyspark.sql.functions import col,current_timestamp

@dlt.table(
    name="sales_enriched_silver"
)
@dlt.expect_or_drop({"valid_margin": "margin_pct BETWEEN 0 AND 90"})
def load_enriched_silver():
    df=spark.readStream.table("sales_silver")
    df=df.withWatermark("sale_date", "1 day")

    static_df=spark.read.table("dim_product")
    joined_df=(df.join(static_df,"product_id")
               .withColumn("profit",col("total_amount") - (col("cost_price") * (col("quantity"))))
               .withColumn("margin_pct", col("profit") * 100 / col("total_amount"))
               .withColumn("enriched_time",current_timestamp())
               )
    
    return joined_df




