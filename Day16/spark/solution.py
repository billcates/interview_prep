from pyspark.sql import functions as f
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

def func(impressions_df,user_info_df):
    agg_df=impressions_df.join(broadcast(user_info_df),"user_id")
    users_df=agg_df.groupBy("country","campaign_id").agg(
        f.approx_count_distinct("user_id").alias("approx_unique_users")
    )
    users_df=users_df.withColumnRenamed("country","country_tmp").withColumnRenamed("campaign_id","campaign_id_tmp")


    hourly_df=agg_df.withColumn("hr",f.date_trunc('hour',f.col("impression_time")))
    cost_df=hourly_df.groupBy("country","campaign_id","hr").agg(
        f.sum("cost").alias("hourly_cost")
    ).orderBy(f.col("hr"))
    
    cost_df=cost_df.filter((f.current_timestamp() - f.col("hr")) <= f.expr("INTERVAL 24 HOURS"))
    cost_agg_df=cost_df.groupBy("country","campaign_id").agg(
        f.sum("hourly_cost").alias("total_cost_last_24h")
    )

    window1=Window.partitionBy("country").orderBy(f.desc("total_cost_last_24h"))
    summary_df=cost_agg_df.withColumn("rank_in_country",f.dense_rank().over(window1))

    summary_df=summary_df.filter(f.col("rank_in_country")<=3)
    summary_df=summary_df.join(users_df,(summary_df.country==users_df.country_tmp) & (summary_df.campaign_id==users_df.campaign_id_tmp))
    return summary_df.drop("country_tmp","campaign_id_tmp")