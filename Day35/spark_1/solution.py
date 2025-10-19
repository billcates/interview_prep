from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as f


spark = SparkSession.builder.appName("Streaming CDC with OPTIMIZE").getOrCreate()

SOURCE_PATH = "s3_path"
TABLE_NAME = "delta_table"
BATCH_OPTIMIZE_INTERVAL = 10  # Run OPTIMIZE every 10 batches

schema = """
product_id STRING,
price DOUBLE,
updated_at TIMESTAMP
"""

def merge(batch_df, batch_id):

    if spark.catalog.tableExists(TABLE_NAME):
        delta_table = DeltaTable.forName(TABLE_NAME)
        target_df = delta_table.toDF()

        joined_df = batch_df.alias("s") \
            .join(target_df.alias("t"), "product_id", "left") \
            .select("s.*", f.col("t.price").alias("t_price"))


        cdc_df = joined_df.filter(
            (f.col("t_price").isNull()) |  # New row → Insert
            (f.col("price") != f.col("t_price"))  # Changed price → Update
        )


        delta_table.alias("t").merge(
            cdc_df.alias("s"),
            "s.product_id = t.product_id"
        ).whenMatchedUpdate(
            condition="s.price != t.price",  
            set={
                "price": f.col("s.price"),
                "updated_at": f.col("s.updated_at")
            }
        ).whenNotMatchedInsert(
            values={
                "product_id": f.col("s.product_id"),
                "price": f.col("s.price"),
                "updated_at": f.col("s.updated_at")
            }
        ).execute()

        # PERIODIC OPTIMIZE WITH ZORDER
        if batch_id % BATCH_OPTIMIZE_INTERVAL == 0 and batch_id != 0:
            print(f"🚀 Running OPTIMIZE ZORDER BY product_id on batch {batch_id}")
            spark.sql(f"""
                OPTIMIZE {TABLE_NAME}
                ZORDER BY (product_id)
            """)

    else:
        print(f"🆕 Table {TABLE_NAME} does not exist. Creating it...")
        batch_df.write.format("delta").saveAsTable(TABLE_NAME)


df = spark.readStream.format("json").schema(schema).load(SOURCE_PATH)

df = df.withWatermark("updated_at", "1 day")

query = df.writeStream \
    .foreachBatch(merge) \
    .outputMode("append") \
    .option("checkpointLocation", "s3://checkpoints/zorder_cdc/") \
    .start()

query.awaitTermination()
