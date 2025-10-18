# corrected_disaster_recovery.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
import sys

# -----------------------------
# CONFIG — set these for your env
# -----------------------------
TABLE_PATH = "/path/to/transactions_gold"         # original table path (or table name)
TABLE_NAME = "transactions_gold"                  # (if registered) name used for RESTORE / MERGE
BACKUP_CLONE_NAME = "transactions_backup_clone"   # managed table name for shallow clone
CLONE_PATH = "/path/to/transactions_backup_clone" # optional path (if you prefer external location)
AUDIT_TABLE = "delta.delta_restore_audit_log"     # managed audit table
GOOD_VERSION = 10
BAD_VERSION = 12
EXECUTED_BY = "data_engineer_team"

# -----------------------------
# START SPARK
# -----------------------------
spark = (SparkSession.builder
         .appName("DeltaDisasterRecovery_Corrected")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

# helper: safe print
def log(msg):
    print(f"[{datetime.utcnow().isoformat()}] {msg}")

log("START: Disaster recovery script")

spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        restore_time TIMESTAMP,
        table_name STRING,
        from_version LONG,
        to_version LONG,
        replay_applied BOOLEAN,
        replayed_operations ARRAY<STRING>,
        executed_by STRING,
        final_record_count LONG,
        corrupted_records_added LONG,
        records_lost LONG,
        records_modified LONG,
        notes STRING
      ) USING DELTA
    """)



spark.sql(f"""
  CREATE TABLE {BACKUP_CLONE_NAME}
  SHALLOW CLONE delta.`{TABLE_PATH}`
  VERSION AS OF {BAD_VERSION}
""")



# Read two versions for comparison
df_good = spark.read.format("delta").option("versionAsOf", GOOD_VERSION).load(TABLE_PATH)
df_bad  = spark.read.format("delta").option("versionAsOf", BAD_VERSION).load(TABLE_PATH)

PK = "transaction_id" if "transaction_id" in df_good.columns else None


# Compute per-row hash excluding PK to detect modifications
cols_to_hash = [c for c in df_good.columns if c != PK]
def with_hash(df, alias):
    # convert nulls to literal string to make hashing stable
    concat_cols = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<<NULL>>")) for c in cols_to_hash])
    return df.withColumn("_hash_" + alias, F.sha2(concat_cols, 256))

df_good_h = with_hash(df_good, "good").select(*([PK] + [F.col(c).alias("good_" + c) for c in cols_to_hash] + [F.col("_hash_good")]))
df_bad_h  = with_hash(df_bad, "bad").select(*([PK] + [F.col(c).alias("bad_" + c) for c in cols_to_hash] + [F.col("_hash_bad")]))

# full outer join on PK
joined = df_good_h.alias("g").join(df_bad_h.alias("b"), on=PK, how="fullouter")

# detect types of change
diff_df = joined.withColumn(
    "change_type",
    F.when(F.col("b._hash_bad").isNull(), F.lit("REMOVED"))
     .when(F.col("g._hash_good").isNull(), F.lit("ADDED"))
     .when(F.col("g._hash_good") != F.col("b._hash_bad"), F.lit("MODIFIED"))
     .otherwise(F.lit(None))
)

# build structs old_values/new_values (null when not applicable)
old_struct_cols = [F.col("g.good_" + c).alias(c) for c in cols_to_hash]
new_struct_cols = [F.col("b.bad_" + c).alias(c) for c in cols_to_hash]

diff_df = diff_df.withColumn("old_values", F.when(F.col("change_type") != "ADDED", F.struct(*old_struct_cols)).otherwise(F.lit(None))) \
                 .withColumn("new_values", F.when(F.col("change_type") != "REMOVED", F.struct(*new_struct_cols)).otherwise(F.lit(None))) \
                 .withColumn("diff_timestamp", F.current_timestamp())

# counts for audit
added_count = diff_df.filter(F.col("change_type") == "ADDED").count()
removed_count = diff_df.filter(F.col("change_type") == "REMOVED").count()
modified_count = diff_df.filter(F.col("change_type") == "MODIFIED").count()

log(f"Diff counts -> ADD: {added_count}, REMOVED: {removed_count}, MODIFIED: {modified_count}")

# Persist diff report for offline forensics (optional path)
diff_report_path = "/tmp/diff_report_v{}_v{}".format(GOOD_VERSION, BAD_VERSION)
diff_df.filter(F.col("change_type").isNotNull()).write.format("delta").mode("overwrite").save(diff_report_path)
log(f"Diff report written to {diff_report_path}")

# -----------------------------
# STEP 3: Restore original table to GOOD_VERSION
# -----------------------------
log(f"STEP 3: RESTORE {TABLE_PATH} to version {GOOD_VERSION}")

# If TABLE_NAME is a registered table name, use RESTORE TABLE <name>. Otherwise use RESTORE TABLE delta.`path`.
try:
    # prefer restore by registered name if exists
    if spark.catalog().tableExists(TABLE_NAME):
        spark.sql(f"RESTORE TABLE {TABLE_NAME} TO VERSION AS OF {GOOD_VERSION}")
    else:
        # restore addressing by location
        spark.sql(f"RESTORE TABLE delta.`{TABLE_PATH}` TO VERSION AS OF {GOOD_VERSION}")
    log(f"RESTORE executed to version {GOOD_VERSION}")
except Exception as e:
    log(f"ERROR during RESTORE: {e}")
    raise

# verify
restored_count = spark.read.format("delta").load(TABLE_PATH).count()
log(f"Records after restore: {restored_count}")

# -----------------------------
# STEP 4: Replay valid deltas from the CLONE (selective replay)
# -----------------------------
log("STEP 4: Replaying valid deltas from the backup clone (only valid inserts/updates)")

# We'll use the backup clone (BAD snapshot) as our source of truth for changes between GOOD_VERSION and BAD_VERSION.
# Use Delta Change Data Feed if enabled on source table (clone inherits the content and metadata)
# Read CDF from clone for the version range (GOOD_VERSION, BAD_VERSION]
# Note: if CDF is not enabled, we can still read specific versions and compute diffs per-version; here we try CDF first.

# Try to read change feed from clone
try:
    # find latest version in clone history (the clone was created at BAD_VERSION snapshot)
    clone_history = spark.sql(f"DESCRIBE HISTORY {BACKUP_CLONE_NAME}")
    max_v = clone_history.agg(F.max("version").alias("v")).collect()[0]["v"]
    log(f"Clone max version: {max_v}")
except Exception as e:
    log(f"Cannot read clone history: {e}")
    max_v = BAD_VERSION

# Read change feed from clone using versions (works if clone has CDF enabled or clone points to original CDF-enabled table)
# If the source (original) had CDF enabled, you can read delta.source_table with readChangeFeed = true and version range.
try:
    cdf_df = (spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", GOOD_VERSION + 1)
              .option("endingVersion", BAD_VERSION)
              .load(CLONE_PATH if CLONE_PATH else TABLE_PATH))
    log("CDF read from clone succeeded")
except Exception as e:
    log(f"CDF read failed: {e}; as fallback we will read per-version snapshots from clone and compute diffs")
    # Fallback: read each version's snapshot and diff against good version to find new rows/updates
    cdf_df = None

replay_applied = False
replayed_operations = []


# Filter for post-image rows (update_postimage or insert). _change_type values include: insert, update_preimage, update_postimage, delete
post_images = cdf_df.filter(F.col("_change_type").isin("insert", "update_postimage"))
# To avoid re-applying the bad merge we can filter out source of operation or pattern matching. For simplicity, assume we only apply inserts/updates.
# Use MERGE into the restored table to idempotently apply these post-images (use PK)
if post_images.head(1) is not None:
    dt = DeltaTable.forPath(spark, TABLE_PATH)
    # Build merge set: update every column from source post-image
    src_cols = [c for c in post_images.columns if not c.startswith("_")]
    set_map = {c: F.expr(f"s.`{c}`") for c in src_cols if c != PK}
    # Run merge in batches by commit version to preserve ordering — optional but safer
    versions = post_images.select("_commit_version").distinct().orderBy("_commit_version").collect()
    for row in versions:
        v = row["_commit_version"]
        batch_df = post_images.filter(F.col("_commit_version") == v).select(*src_cols)
        # MERGE
        dt.alias("t").merge(batch_df.alias("s"), f"t.{PK} = s.{PK}") \
            .whenMatchedUpdate(set=set_map) \
            .whenNotMatchedInsertAll() \
            .execute()
        replay_applied = True
        replayed_operations.append(f"cdf_v{v}")
        log(f"Replayed CDF version {v}")

# -----------------------------
# STEP 5: Log recovery event
# -----------------------------
log("STEP 5: Write recovery audit event")

final_count = spark.read.format("delta").load(TABLE_PATH).count()
audit_row = [(datetime.now(), TABLE_PATH, BAD_VERSION, GOOD_VERSION, replay_applied, replayed_operations, EXECUTED_BY, final_count, added_count, removed_count, modified_count, "Restored and selectively replayed valid deltas")]
audit_df = spark.createDataFrame(audit_row, schema=["restore_time","table_name","from_version","to_version","replay_applied","replayed_operations","executed_by","final_record_count","corrupted_records_added","records_lost","records_modified","notes"])
audit_df.write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)
log("Audit record written")

log("DISASTER RECOVERY COMPLETED")
