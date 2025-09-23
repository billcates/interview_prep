from pyspark.sql.window import Window
from pyspark.sql import functions as f

def func(df):
    window=Window().partitionBy("user_id","activity_type").orderBy("activity_date")

    df=df.withColumn("previous_date",f.lag("activity_date").over(window))
    df = df.withColumn(
    "flag",
    f.when(
        (f.col("previous_date").isNull()) | (f.datediff("activity_date", "previous_date") > 1),
        1
    ).otherwise(0)
    )

    cum_window = (
    Window.partitionBy("user_id", "activity_type")
          .orderBy("activity_date")
          .rowsBetween(Window.unboundedPreceding, 0) # in pyspark explicit definition is needed
    )

    df=df.withColumn("streak_id",f.sum(f.col("flag")).over(cum_window)).drop("flag").drop("previous_date")
    agg_df=df.groupBy("user_id","activity_type","streak_id").agg(
        f.min(f.col("activity_date")).alias("streak_start_date"),
        f.max(f.col("activity_date")).alias("streak_end_date"),
        f.count(f.col("activity_date")).alias("streak_length")
    )
    return agg_df