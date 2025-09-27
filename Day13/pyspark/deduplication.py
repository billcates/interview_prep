from pyspark.sql import functions as f
from pyspark.sql.window import Window

def func(click_stream_df,orders_df):
    click_stream_df=click_stream_df.withColumn("user_id_click","user_id")

    join_cond = (
        (click_stream_df.user_id_click == orders_df.user_id) &
        (click_stream_df.event_time < orders_df.order_time) &
        ((f.unix_timestamp(orders_df.order_time)-f.unix_timestamp(click_stream_df.event_time)) < 3600)
    )
    click_orders_df = click_stream_df.join(orders_df, join_cond, "inner")

    agg_df=click_orders_df.drop("user_id_click")
    
    window=Window.partitionBy("order_id").orderBy(f.desc("event_time"))
    agg_df=agg_df.withColumn("rk",f.row_number().over(window)).filter(f.col("rk")==1).drop("rk")
    campaign_df=agg_df.withColumn('campaign_id',f.regexp_extract("referrer",'[?&/]campaign=([^&/]+)',1))

    result_df=campaign_df.groupBy("campaign_id").agg(
        f.sum("revenue").alias("total_revenue"),
        f.countDistinct("user_id").alias("unique_buyers"),
        f.avg("revenue").alias("avg_revenue_per_order")
    )
    return result_df
