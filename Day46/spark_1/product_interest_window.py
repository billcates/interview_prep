from pyspark.sql.functions import col, when, min

def calculate_interest_windows(df):
    df = (df
          .withColumn("view_time", when(col("event_type") == "view", col("event_time")))
          .withColumn("purchase_time", when(col("event_type") == "purchase", col("event_time")))
         )

    df = df.groupBy("user_id", "product_id").agg(
        min("view_time").alias("first_view_time"),
        min("purchase_time").alias("purchase_time")
    )

    result_df = (df
                 .filter(col("purchase_time").isNotNull())
                 .withColumn("interest_duration_minutes",
                             (col("purchase_time").cast("long") - col("first_view_time").cast("long")) / 60)
                )

    return result_df.orderBy("interest_duration_minutes")
