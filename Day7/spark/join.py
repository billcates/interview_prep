from pyspark.sql import functions as f
from pyspark.sql.window import Window

def func(transactions_df,products_df):
    agg_df=transactions_df.join(products_df,"product_id")
    agg_df=agg_df.filter(f.datediff(f.current_date(),f.to_date(f.col("transaction_date")))<30)
    summary_df=agg_df.groupBy("customer_id","category").agg(
        f.sum(f.col("amount")).alias("total_spend_last_30_days"),
        f.countDistinct(f.col("product_id")).alias("unique_products_last_30_days")
    )
    window=Window().partitionBy("category").orderBy(f.desc("total_spend_last_30_days"))
    summary_df=summary_df.withColumn("rank_in_category",f.rank().over(window))
    return summary_df