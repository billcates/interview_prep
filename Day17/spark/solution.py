from pyspark.sql import functions as f
from pyspark.sql.window import Window
num_salts=25

def func(transactions_df,users_df):
    agg_df=transactions_df.join(f.broadcast(users_df),"user_id")
    
    agg_df=agg_df.filter((f.current_timestamp() - f.col("timestamp")) <= f.expr("INTERVAL 7 DAYS"))

    df_salted = agg_df.withColumn("salt", (f.rand() * num_salts).cast("int"))

    summary_df=df_salted.groupBy("country","user_id","salt").agg(
        f.sum(f.col("amount")).alias("total_amount")
    )
    summary_df=summary_df.groupBy("country","user_id").agg(
        f.sum(f.col("total_amount")).alias("total_amount")
    )

    window=Window.partitionBy("country").orderBy(f.desc("overall_amount"))
    result_df=summary_df.withColumn("rk",f.row_number().over(window)).filter(f.col("rk")<=5).drop("rk")
    return result_df