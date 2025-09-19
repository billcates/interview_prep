from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark=SparkSession.builder().appName("ranking").getOrCreate()

def func(clicks_df,products_df):
    agg_df=(clicks_df.join(products_df,"page_url")
    .filter(f.datediff(f.current_date(),f.col("click_time"))<=7))
    agg_df=agg_df.groupBy("user_id","product_category").agg(f.count(f.col("click_time")).alias("total_clicks_last_7_days"))


    window=Window().partitionBy("product_category").orderBy(f.desc("total_clicks_last_7_days"))
    summary_df=agg_df.withColumn("rank_in_category",f.rank().over(window))
    return summary_df