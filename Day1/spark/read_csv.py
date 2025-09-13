from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,desc,sum as _sum


spark=SparkSession.builder.appName("read_csv").getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),        
    StructField("amount", DecimalType(10,2), True),      
    StructField("transaction_date", StringType(), True),           
])


df=spark.read.schema(schema).csv("path")
df=df.repartition(10,"customer_id")
customer_df_summary_df=df.groupBy(col("customer_id"),col("transaction_date")).agg(_sum(col("amount")).alias("total_amount")).orderBy(desc("total_amount"))
customer_df_summary_df.write.partitionBy("transaction_date").mode("overwrite").parquet("write_path")
