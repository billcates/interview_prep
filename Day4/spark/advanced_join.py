from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType, StringType, DateType
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark=SparkSession.builder().appName("Advanced Join").getOrCreate()

customer_schema=StructType([
    StructField("customer_id",StringType(),True),
    StructField("name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("signup_date",DateType(),True)
])

order_schema=StructType([
    StructField("order_id",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_amount",StringType(),True),
    StructField("order_date",DateType(),True)
])

returns_schema=StructType([
    StructField("return_id",StringType(),True),
    StructField("order_id",StringType(),True),
    StructField("return_date",DateType(),True)
])

customer_df= spark.read.option("header","true").schema(customer_schema).csv("customers.csv")
orders_df=spark.read.option("header","true").schema(order_schema).csv("order.csv")
returns_df=spark.read.option("header","true").schema(returns_schema).csv("returns.csv")

orders_df = orders_df.withColumn("order_amount", f.col("order_amount").cast("double"))

agg_df=(customer_df.join(orders_df,"customer_id")
    .join(returns_df,"order_id","left"))

#  | customer_id | name | city | signup_date || order_id | order_amount | order_date || return_id  | return_date |

filtered_df=agg_df.filter((f.col("order_date").isNotNull()) & (f.datediff(f.current_date(), f.col("order_date"))<90))
customer_summary_df=filtered_df.groupBy(f.col("city"),f.col("customer_id")).agg(
    f.count("*").alias("total_order_count"),
    f.sum(f.col("order_amount")).alias("total_order_amount"),
    f.count("return_id").alias("total_returned_orders")
    )

window=Window().partitionBy("city").orderBy(f.desc("total_order_amount"))
summary_df=customer_summary_df.withColumn("loyalty_rank",f.rank().over(window))

summary_df.write.partitionBy("city","loyalty_rank").mode("overwrite").parquet("output_path")