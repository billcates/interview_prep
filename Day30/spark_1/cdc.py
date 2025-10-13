from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, to_timestamp
import time

BRONZE_PATH = "/tmp/delta/bronze_txns"      # replace with s3://your-bucket/delta/bronze_txns
SILVER_TABLE = "silvertable"
BRONZE_TABLE = "bronze_table"


spark = (
    SparkSession.builder
    .appName("DeltaCDFExample")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")


spark.sql(f"""
CREATE TABLE {BRONZE_TABLE} (
  txn_id STRING,
  user_id STRING,
  amount DOUBLE,
  status STRING,
  event_time TIMESTAMP
)
USING DELTA
LOCATION '{BRONZE_PATH}'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("Created bronze table with CDF enabled.")


def short_sleep():
    time.sleep(1.0)


initial_data = [
    ("1", "alice", 10.0, "NEW", "2025-10-01 10:00:00"),
    ("2", "bob",   20.0, "NEW", "2025-10-01 10:05:00"),
    ("3", "carol", 30.0, "NEW", "2025-10-01 10:10:00"),
    ("4", "dave",  40.0, "NEW", "2025-10-01 10:15:00")
]
df0 = spark.createDataFrame(initial_data, schema=["txn_id","user_id","amount","status","event_time"]) \
           .withColumn("event_time", to_timestamp("event_time"))
df0.write.format("delta").mode("append").save(BRONZE_PATH)
short_sleep()
print("Commit 1: initial rows inserted.")


df1 = spark.createDataFrame([
    ("5", "eve", 50.0, "NEW", "2025-10-01 10:20:00"),
    ("6", "frank", 60.0, "NEW", "2025-10-01 10:25:00")
], schema=["txn_id","user_id","amount","status","event_time"]).withColumn("event_time", to_timestamp("event_time"))

df1.write.format("delta").mode("append").save(BRONZE_PATH)
short_sleep()
print("Commit 2: two new rows inserted.")


bronze_delta = DeltaTable.forPath(spark, BRONZE_PATH)
bronze_delta.update(
    condition = "txn_id = '3'",
    set = { "amount": "100.0", "status": "'CORRECTED'" }
)
short_sleep()
print("Commit 3: updated txn_id=3.")

bronze_delta.delete("txn_id = '2'")
short_sleep()
print("Commit 4: deleted txn_id=2.")


print("Bronze table history:")
spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").show(truncate=False)

history_df = spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}")
latest_version = history_df.selectExpr("max(version) as v").collect()[0]["v"]
print(f"Latest bronze version = {latest_version}")

cdf_df = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)          # read from version 0
    .option("endingVersion", latest_version)  # up to latest committed version
    .load(BRONZE_PATH)
)

# CDF exposes metadata columns: _change_type, _commit_version, _commit_timestamp
print("Sample of Change Data Feed rows:")
silver_df=cdf_df.select(
    "txn_id","user_id","amount","status","event_time","_change_type","_commit_version","_commit_timestamp"
).show(truncate=False)


# Write silver as managed Delta table
silver_df.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
print("Silver table written.")

print("Silver table contents:")
spark.sql(f"SELECT *, _change_type, _commit_version, _commit_timestamp FROM {SILVER_TABLE} ORDER BY _commit_version, txn_id").show(truncate=False)

print("Final Bronze history:")
spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").show(truncate=False)
