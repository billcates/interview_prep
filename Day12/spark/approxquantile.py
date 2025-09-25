from pyspark.sql import functions as F

def critical_urls(df):
    df = df.withColumn("is_failed", F.when(F.col("status_code") >= 400, 1).otherwise(0))
    agg_df = df.groupBy("url").agg(
        F.expr("percentile_approx(response_time, 0.95)").alias("p95_response_time"),
        (F.sum("is_failed") / F.count("*") * 100).alias("failure_percentage")
    )

    critical_df = agg_df.filter(F.col("failure_percentage") > 1)
    critical_df = critical_df.orderBy(F.desc("p95_response_time"))
    
    return critical_df
