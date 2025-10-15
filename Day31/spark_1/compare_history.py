from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_VERSION = 3
TARGET_VERSION = 5
DELTA_TABLE_PATH = "s3://your-delta-path/transactions_delta"
AUDIT_TABLE = "delta.txn_audit_log"

spark = (
    SparkSession.builder
    .appName("Delta Time Travel Diff Audit")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df_base = spark.read.format("delta").option("versionAsOf", BASE_VERSION).load(DELTA_TABLE_PATH)
df_target = spark.read.format("delta").option("versionAsOf", TARGET_VERSION).load(DELTA_TABLE_PATH)

pk_col = "txn_id"
base_cols = [c for c in df_base.columns if c != pk_col]
target_cols = [c for c in df_target.columns if c != pk_col]

df_base = df_base.withColumn("_hash", F.sha1(F.concat_ws("||", *base_cols)))
df_target = df_target.withColumn("_hash", F.sha1(F.concat_ws("||", *target_cols)))


df_join = df_base.alias("b").join(df_target.alias("t"), on=pk_col, how="fullouter")


df_diff = (
    df_join
    .withColumn(
        "change_type",
        F.when(F.col("t._hash").isNull(), "REMOVED")
         .when(F.col("b._hash").isNull(), "ADDED")
         .when(F.col("b._hash") != F.col("t._hash"), "MODIFIED")
         .otherwise(None)
    )
)


df_diff = (
    df_diff
    .withColumn(
        "old_values",
        F.when(F.col("change_type") != "ADDED", F.struct(*[F.col(f"b.{c}") for c in base_cols]))
    )
    .withColumn(
        "new_values",
        F.when(F.col("change_type") != "REMOVED", F.struct(*[F.col(f"t.{c}") for c in target_cols]))
    )
    .withColumn("diff_timestamp", F.current_timestamp())
)

df_final = df_diff.select(
    F.coalesce(F.col("b.txn_id"), F.col("t.txn_id")).alias("txn_id"),
    "change_type",
    "old_values",
    "new_values",
    F.lit(BASE_VERSION).alias("base_version"),
    F.lit(TARGET_VERSION).alias("target_version"),
    "diff_timestamp"
)

df_final.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
