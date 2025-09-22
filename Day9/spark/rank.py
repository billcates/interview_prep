# You have two datasets:

# transactions_df
# transaction_id (string)
# customer_id (string)
# product_id (string)
# amount (decimal)
# transaction_date (timestamp)

# products_df
# product_id (string)
# category (string)

# Task

# For each customer, compute their top 2 categories by total spend in the last 60 days.
# For each customer-category combination, compute:
# total_amount spent
# total_transactions count

# The final DataFrame should have:

# customer_id
# category
# total_amount
# total_transactions
# rank_in_customer (rank the categories per customer by total_amount)
from pyspark.sql import functions as f
from pyspark.sql.window import Window

def func(transactions_df,products_df):
    agg_df=transactions_df.join(products_df,"product_id")
    agg_df=agg_df.filter(f.datediff(f.current_date(), f.col("transaction_date"))<=60)
    summary_df=agg_df.groupBy("customer_id","category").agg(
        f.sum(f.col("amount")).alias("total_amount"),
        f.count(f.col("*")).alias("total_transactions")
    )

    window=Window().partitionBy("customer_id").orderBy(f.desc("total_amount"))
    rank_df=summary_df.withColumn("rank_in_customer",f.rank().over(window))
    rank_df=rank_df.filter(f.col("rank_in_customer")<=2)
    return rank_df