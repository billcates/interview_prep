from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import desc,col,row_number

spark=SparkSession.builder().appName("Top 2 most recent orders").getOrCreate()

df=spark.read.format("delta").load("/mnt/delta/orders/")

window=Window.partitionBy(col("customer_id")).orderBy(desc(col("order_date"), desc(col("order_amount"))))
df=df.withColumn("rn",row_number().over(window))
df=df.filter(col("rn")<=2)

df.write.partitionBy("customer_id").mode("overwrite").parquet("/mnt/parquet/top_orders/")