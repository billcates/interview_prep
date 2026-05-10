from pyspark.sql.functions import Window,lag,col

def calculate_order_difference(df):

    window=Window().partitionBy("customer_id").orderBy("order_date")

    df=(df.withColumn("prev_amount",lag("amount",default=0).over(window))
    .withColumn("diff",col("amount")-col("prev_amount"))
    )

    return df