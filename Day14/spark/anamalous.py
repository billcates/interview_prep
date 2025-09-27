from pyspark.sql import functions as f
from pyspark.sql.window import Window

def func(telemetry_df,device_info_df):
    agg_df=telemetry_df.join(device_info_df,"device_id")
    agg_df = agg_df.withColumn("event_ts", f.col("event_time").cast("long"))

    window=Window.partitionBy("device_id").orderBy("event_ts").rangeBetween(-900,0) ##learning rangebetween to filter the values which falls in this range
    agg_df=agg_df.withColumn("rolling_avg_temp",f.avg("temperature").over(window)).withColumn("rolling_min_battery", f.min("battery_level").over(window))

    window1=Window.partitionBy("device_id","region").orderBy("event_ts").rangeBetween(-3600,0)
    summary_df=agg_df.withColumn("rolling_error_rate",(f.count("error_code").over(window1)*100.0)/(f.count("*").over(window1)))

    summary_df=summary_df.withColumn("is_anomalous",f.when((f.col("rolling_avg_temp")>80) | (f.col("rolling_min_battery")<10) |(f.col("rolling_error_rate") >5),1).otherwise(0))
    return summary_df.select("device_id","region","model","rolling_avg_temp","rolling_min_battery","rolling_error_rate","is_anomalous")