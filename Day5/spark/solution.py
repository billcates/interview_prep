from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

spark=SparkSession.builder().appName("sales summary").getOrCreate()

def func(sales_df,products_df):
    summary_df= sales_df.join(products_df,"product_id")
    agg_summary_df = (summary_df.groupBy("category","product_id").agg(
        f.sum(f.col("quantity")).alias("t_quantity"),
        f.sum(f.col("price") * f.col("quantity")).alias("t_sales_amount")
    ))

    window=Window().partitionBy("category").orderBy(f.desc("t_sales_amount"))
    result_df=agg_summary_df.withColumn("rn",f.rank().over(window))
    top_products_df= result_df.filter(f.col("rn")==1).drop(f.col("rn")).withColumnRenamed("product_id","top_product_id")
    top_products_df=top_products_df.select("top_product_id","category")

    result_df=agg_summary_df.groupBy("category").agg(
        f.sum(f.col("t_quantity")).alias("total_quantity"),
        f.sum(f.col("t_sales_amount")).alias("total_sales_amount")    
    )
    return result_df.join(top_products_df,"category")
    