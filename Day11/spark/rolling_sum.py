from pyspark.sql.window import Window
from pyspark.sql import functions as f

def func(orders_df,products_df):
    agg_df=orders_df.join(products_df,"product_id")
    orders_with_ts = agg_df.withColumn("order_ts", f.col("order_date").cast("timestamp").cast("long"))
    window = (
        Window.partitionBy("user_id","category")
            .orderBy("order_ts")
            .rangeBetween(-6*86400, 0)
    )
    
    agg_df=orders_with_ts.withColumn("rolling_7d_amount",f.sum(f.col("amount")).over(window))

    window2=Window.partitionBy("user_id","order_date").orderBy(f.desc("rolling_7d_amount"))
    rank_df=agg_df.withColumn("rank_in_user",f.rank().over(window2))
    result_df=rank_df.filter(f.col("rank_in_user")<=2).select("user_id", "order_date", "category", "rolling_7d_amount", "rank_in_user")
    return result_df