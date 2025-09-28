from pyspark.sql.window import Window
from pyspark.sql import functions as f

def func(sensor_readings_df,maintenance_logs_df):
    window=Window.partitionBy("device_id","event_time").orderBy(f.desc("ingestion_time"))

    sensor_readings_df= sensor_readings_df.withColumn("rn",f.row_number().over(window)).filter(f.col("rn")==1).drop("rn")
    sensor_readings_df=sensor_readings_df.withColumn("sensor_device_id","device_id")

    maintenance_df=sensor_readings_df.join(maintenance_logs_df,(
        (sensor_readings_df.sensor_device_id==maintenance_logs_df.device_id)
        & (sensor_readings_df.event_time> maintenance_logs_df.maintenance_time)
        & ((f.unix_timestamp(sensor_readings_df.event_time)-f.unix_timestamp(maintenance_logs_df.maintenance_time)) < 86400)),"left")
    
    window1=Window.partitionBy("device_id","event_time").orderBy(f.desc("maintenance_time"))
    maintenance_df=maintenance_df.withColumn("rk",f.row_number().over(window1)).filter(f.col("rk")==1).drop("rk")
    maintenance_df = maintenance_df.withColumn("maintenance_type",
                               f.when(f.col("maintenance_type").isNull(),"none")
                               .otherwise(f.col("maintenance_type")))


    maintenance_df=maintenance_df.withColumn("event_ts",f.col("event_time").cast("long"))
    window3=Window.partitionBy("device_id").orderBy("event_ts").rangeBetween(-3600,0)
    
    summary_df=(maintenance_df.withColumn("rolling_avg_temperature",f.avg("temperature").over(window3))
                .withColumn("rolling_max_vibration",f.max("vibration").over(window3))
                .withColumn("rolling_std_pressure",f.stddev("pressure").over(window3)))
    
    summary_df=(summary_df.withColumn("is_anomalous", 
                f.when((f.col("rolling_avg_temperature")>90) | (f.col("rolling_max_vibration")> 50) | 
                       ((f.col("maintenance_time")=="none") &( f.col("rolling_std_pressure") > 10)),1).otherwise(0)
                ))

    return (summary_df.select("device_id","event_time","maintenance_type","rolling_avg_temperature","rolling_max_vibration","rolling_std_pressure","is_anomalous")
            .orderBy("device_id","event_time"))