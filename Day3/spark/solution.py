from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark=SparkSession.builder().appName("basic join").getOrCreate()

df1_schema = StructType([
    StructField("customer_id",StringType(), True),
    StructField("name",StringType(), True),
    StructField("city",StringType(), True),
    StructField("signup_date",StringType(), True)
])

df2_schema = StructType([
    StructField("order_id",StringType(), True),
    StructField("customer_id",StringType(), True),
    StructField("order_amount",DecimalType(10,2), True),
    StructField("order_date",StringType(), True)
])

df1 = spark.read.format("csv").schema(df1_schema).load("/mnt/data/customers.csv")
df2 = spark.read.format("csv").schema(df2_schema).load("/mnt/data/orders.csv")

summary_df= df1.join(df2,"customer_id")
summary_df = summary_df.withColumn("signup_date",f.to_date(f.col("signup_date")))
summary_df = summary_df.withColumn("days_since_joined",f.datediff(f.current_date(),f.col("signup_date")))
summary_df = summary_df.filter(f.col("days_since_joined")<=30)

city_df= summary_df.groupBy("city").agg(f.avg(f.col("order_amount")).alias("avg_order_amount"))
city_df.write.format("delta").partitionBy("city").mode("overwrite").save("/mnt/delta/city_order_summary/")