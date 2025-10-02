from pyspark.sql import functions as f

def func(impressions_df, clicks_df):
    impressions_df = impressions_df.withWatermark("impression_time", "15 minutes").dropDuplicates(["impression_id"])
    
    clicks_df = clicks_df.withWatermark("click_time", "30 minutes").dropDuplicates(["click_id"])
    
    agg_df = impressions_df.alias("imp").join(
        clicks_df.alias("clk"),
        (f.col("imp.impression_id") == f.col("clk.impression_id")) &
        (f.col("clk.click_time") >= f.col("imp.impression_time")) &
        (f.col("clk.click_time") <= f.col("imp.impression_time") + f.expr("INTERVAL 1 HOUR")),
        "left"
    )

    agg_df = agg_df.groupBy(
        f.col("imp.campaign_id").alias("campaign_id"),
        f.window(f.col("imp.impression_time"), "10 minutes")
    ).agg(
        f.count(f.col("imp.impression_id")).alias("total_impressions"),
        f.count(f.col("clk.click_id")).alias("total_clicks"), 
        
        f.when(
            f.count(f.col("imp.impression_id")) > 0,
            f.count(f.col("clk.click_id")) / f.count(f.col("imp.impression_id"))
        ).otherwise(0).alias("ctr"),
        
        f.sum(f.col("imp.cost")).alias("total_spend"),
        
        f.when(
            f.count(f.col("clk.click_id")) > 0,
            f.sum(f.col("imp.cost")) / f.count(f.col("clk.click_id"))
        ).otherwise(None).alias("cpc")
    ).select(
        "campaign_id",
        f.col("window.start").alias("window_start"),
        f.col("window.end").alias("window_end"),
        "total_impressions",
        "total_clicks",
        "ctr",
        "total_spend",
        "cpc"
    )
    
    return agg_df