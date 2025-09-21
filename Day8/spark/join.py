from pyspark.sql import functions as f
from pyspark.sql.window import Window

def func(df):
    window=Window().partitionBy("user_id").orderBy("click_time")
    df=df.withColumn("previous_click_time",f.lag(f.col("click_time")).over(window))
    df=df.withColumn("flag",f.when((f.col("previous_click_time").isNull()) | ((f.col("click_time").cast("long") - f.col("previous_click_time").cast("long")) / 60)>30, 1).otherwise(0))
    
    df=df.withColumn("session_id",f.sum("flag").over(window)).drop("flag")
    summary_df=df.groupBy("session_id","user_id").agg(f.count(f.col("click_time")).alias("total_number_of_clicks"),
                                            f.min(f.col("click_time")).alias("first_click_time"),
                                            f.max(f.col("click_time")).alias("last_click_time"))
    summary_df=summary_df.withColumn("session_duration",(f.col("last_click_time").cast("long") - f.col("first_click_time").cast("long"))/60)
    return summary_df

