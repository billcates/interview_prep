from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType, DoubleType, ArrayType
import datetime
import json
import sys
import traceback


DEFAULT_AUDIT_TABLE = "delta.delta_maintenance_audit"   # managed audit table name
IDEAL_MIN_FILE_MB = 64          # if avg < this, recommend OPTIMIZE
OPTIMIZE_FILE_THRESHOLD_MB = 32 # stricter threshold to trigger optimize
OPTIMIZE_NUMFILES_THRESHOLD = 5000
OPTIMIZE_MAX_AGE_HOURS = 72     # trigger if last optimize older than this
VACUUM_MIN_DAYS = 7             # consider vacuum if last vacuum older than this
VACUUM_RECLAIM_PCT = 0.05       # require reclaimable > 5% to actually vacuum
VACUUM_RETAIN_HOURS = 168       # default retention (7 days) when executing vacuum
DRY_RUN_STRING = "DRY RUN"


spark = SparkSession.builder \
    .appName("DeltaMaintenanceAdvisor") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

def now_iso():
    return datetime.datetime.utcnow().isoformat()

def ensure_audit_table(audit_table=DEFAULT_AUDIT_TABLE):
    """Create audit table if not exists (managed Delta table)."""
    try:
        catalog_parts = audit_table.split(".")
        if len(catalog_parts) == 2:
            db, tbl = catalog_parts
            exists = spark.catalog().tableExists(db, tbl)
        else:
            exists = spark.catalog().tableExists(None, audit_table)
    except Exception:
        exists = False

    if not exists:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {audit_table} (
                run_time TIMESTAMP,
                table_path STRING,
                action STRING,
                reason STRING,
                before_metrics MAP<STRING, STRING>,
                after_metrics MAP<STRING, STRING>,
                executed_by STRING,
                optimize_executed BOOLEAN,
                vacuum_executed BOOLEAN,
                reclaimable_bytes LONG,
                total_size_bytes LONG,
                notes STRING
            ) USING DELTA
        """)
        print(f"[{now_iso()}] Created audit table: {audit_table}")

def describe_table_detail(table_path):
    """Return a dict with fields from DESCRIBE DETAIL delta.`path`"""
    detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
    detail = detail_df.collect()[0].asDict()
    # normalize keys we care about
    return {
        "path": detail.get("location"),
        "numFiles": int(detail.get("numFiles") or 0),
        "sizeInBytes": int(detail.get("sizeInBytes") or 0),
        "partitionColumns": detail.get("partitionColumns") or [],
        "format": detail.get("format") or None
    }

def get_recent_history(table_path, limit=50):
    """Return DataFrame of DESCRIBE HISTORY for the table_path"""
    hist_df = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}` LIMIT {limit}")
    return hist_df

def last_operation_time(history_df, op_name_substring):
    """Find most recent operation that contains substring in operation column and return timestamp (UTC)"""
    try:
        rows = history_df.filter(F.col("operation").contains(op_name_substring)).select("version","timestamp").orderBy(F.col("version").desc()).collect()
        if not rows:
            return None, None
        row = rows[0]
        return row["version"], row["timestamp"]
    except Exception:
        return None, None

def run_optimize(table_path, zorder_cols=None):
    """Run OPTIMIZE <table> ZORDER BY (cols) — Databricks only"""
    if zorder_cols and len(zorder_cols) > 0:
        zcols = ", ".join(zorder_cols)
        print(f"[{now_iso()}] Running OPTIMIZE ZORDER BY ({zcols}) on {table_path}")
        spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zcols})")
    else:
        print(f"[{now_iso()}] Running OPTIMIZE on {table_path}")
        spark.sql(f"OPTIMIZE delta.`{table_path}`")

def run_vacuum_dry_run(table_path, retain_hours):
    """Run VACUUM ... DRY RUN and attempt to return reclaimable bytes.
       Databricks returns a result set for DRY RUN listing files — attempt to sum their sizes.
    """
    try:
        # This command may return rows (Databricks) or nothing (OSS). We'll attempt to capture DataFrame.
        dry_df = spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retain_hours} HOURS DRY RUN")
        cols = dry_df.columns
        # Try common column names
        size_col = None
        for candidate in ("size", "fileSize", "file_size", "sizeInBytes"):
            if candidate in cols:
                size_col = candidate
                break
        reclaimable = 0
        if size_col:
            reclaimable = dry_df.agg(F.sum(F.col(size_col)).cast("long")).collect()[0][0] or 0
        else:
            # If the DRY RUN returned file paths only, attempt to parse and sum sizes by reading file lengths via filesystem
            # This is best-effort; may not work on all storage backends.
            try:
                file_paths = [r[0] for r in dry_df.collect()] if dry_df.count() > 0 else []
                reclaimable = 0
                for p in file_paths:
                    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
                    pth = spark._jvm.org.apache.hadoop.fs.Path(p)
                    if fs.exists(pth):
                        reclaimable += fs.getFileStatus(pth).getLen()
            except Exception:
                reclaimable = 0
        print(f"[{now_iso()}] VACUUM DRY RUN reclaimable bytes (est): {reclaimable}")
        return int(reclaimable)
    except Exception as e:
        # On many environments, VACUUM DRY RUN might not return a dataset; log and return -1 (unknown)
        print(f"[{now_iso()}] VACUUM DRY RUN failed or returned nothing: {e}")
        return -1

def run_vacuum(table_path, retain_hours):
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retain_hours} HOURS")


def write_audit_row(audit_table, row: dict):
    """Append row (a dict) to audit_table (Delta managed table)"""
    df = spark.createDataFrame([row], schema=[
        StructField("run_time", TimestampType(), True),
        StructField("table_path", StringType(), True),
        StructField("action", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("before_metrics", MapType(StringType(), StringType()), True) if False else StructField("before_metrics", StringType(), True),
        StructField("after_metrics", StringType(), True),
        StructField("executed_by", StringType(), True),
        StructField("optimize_executed", BooleanType(), True),
        StructField("vacuum_executed", BooleanType(), True),
        StructField("reclaimable_bytes", LongType(), True),
        StructField("total_size_bytes", LongType(), True),
        StructField("notes", StringType(), True),
    ])
    # We store before/after metrics as JSON strings to avoid complex schema issues
    df = df.select(
        F.lit(row["run_time"]).cast(TimestampType()).alias("run_time"),
        F.lit(row["table_path"]).alias("table_path"),
        F.lit(row["action"]).alias("action"),
        F.lit(row["reason"]).alias("reason"),
        F.lit(json.dumps(row.get("before_metrics", {}))).alias("before_metrics"),
        F.lit(json.dumps(row.get("after_metrics", {}))).alias("after_metrics"),
        F.lit(row.get("executed_by", "advisor")).alias("executed_by"),
        F.lit(row.get("optimize_executed", False)).cast(BooleanType()).alias("optimize_executed"),
        F.lit(row.get("vacuum_executed", False)).cast(BooleanType()).alias("vacuum_executed"),
        F.lit(row.get("reclaimable_bytes", 0)).cast(LongType()).alias("reclaimable_bytes"),
        F.lit(row.get("total_size_bytes", 0)).cast(LongType()).alias("total_size_bytes"),
        F.lit(row.get("notes", "")).alias("notes")
    )
    df.write.format("delta").mode("append").saveAsTable(audit_table)
    print(f"[{now_iso()}] Wrote audit row to {audit_table}")


def maintain_table(table_path,
                   audit_table=DEFAULT_AUDIT_TABLE,
                   zorder_cols=None,
                   ideal_min_mb=IDEAL_MIN_FILE_MB,
                   optimize_numfiles_threshold=OPTIMIZE_NUMFILES_THRESHOLD,
                   optimize_file_threshold_mb=OPTIMIZE_FILE_THRESHOLD_MB,
                   optimize_max_age_hours=OPTIMIZE_MAX_AGE_HOURS,
                   vacuum_min_days=VACUUM_MIN_DAYS,
                   vacuum_reclaim_pct=VACUUM_RECLAIM_PCT,
                   vacuum_retain_hours=VACUUM_RETAIN_HOURS,
                   executed_by="advisor"):
    """
    Inspect the table, optionally run OPTIMIZE and VACUUM, and log audit rows.
    """
    ensure_audit_table(audit_table)


    detail = describe_table_detail(table_path)
    total_files = detail["numFiles"]
    total_size = detail["sizeInBytes"]
    avg_file_mb = (total_size / total_files) / (1024 * 1024) if total_files > 0 else 0
    partition_cols = detail["partitionColumns"]

    history_df = get_recent_history(table_path)
    last_opt_ver, last_opt_time = last_operation_time(history_df, "OPTIMIZE")
    last_vac_ver, last_vac_time = last_operation_time(history_df, "VACUUM")

    # compute ages (hours/days)
    now = datetime.datetime.utcnow()
    def hours_since(ts):
        if ts is None:
            return None
        if isinstance(ts, str):
            ts = datetime.datetime.fromisoformat(ts)
        return (now - ts).total_seconds() / 3600.0

    last_opt_hours = hours_since(last_opt_time)
    last_vac_days = None
    if last_vac_time is not None:
        last_vac_days = (now - last_vac_time).days

    # Decision logic for OPTIMIZE
    optimize_reason = None
    optimize_needed = False
    if avg_file_mb < optimize_file_threshold_mb:
        optimize_needed = True
        optimize_reason = f"avg_file_mb {avg_file_mb:.2f} < threshold {optimize_file_threshold_mb}"
    elif total_files > optimize_numfiles_threshold:
        optimize_needed = True
        optimize_reason = f"numFiles {total_files} > threshold {optimize_numfiles_threshold}"
    elif last_opt_hours is None or last_opt_hours > optimize_max_age_hours:
        optimize_needed = True
        optimize_reason = f"last_opt older than {optimize_max_age_hours} hours (last_opt_hours={last_opt_hours})"

    # Audit baseline metrics
    before_metrics = {
        "numFiles": total_files,
        "sizeInBytes": total_size,
        "avgFileMB": round(avg_file_mb, 2),
        "partitionColumns": partition_cols,
        "lastOptimizeVersion": last_opt_ver,
        "lastOptimizeTime": str(last_opt_time),
        "lastVacuumVersion": last_vac_ver,
        "lastVacuumTime": str(last_vac_time)
    }

    optimize_executed = False
    if optimize_needed:
        try:
            run_optimize(table_path, zorder_cols=zorder_cols)
            optimize_executed = True
        except Exception as e:
            print(f"[{now_iso()}] ERROR during OPTIMIZE: {e}")
            traceback.print_exc()
    else:
        print(f"[{now_iso()}] OPTIMIZE skipped: no trigger conditions met ({optimize_reason})")

    # After optimize, refresh detail
    try:
        detail_after = describe_table_detail(table_path)
    except Exception:
        detail_after = detail

    # VACUUM decision
    vacuum_executed = False
    reclaimable_bytes = -1
    vacuum_reason = None
    # Only check DRY RUN if last vacuum older than min days or unknown
    if last_vac_time is None or (last_vac_days is None) or (last_vac_days > vacuum_min_days):
        reclaimable_bytes = run_vacuum_dry_run(table_path, vacuum_retain_hours)
        if reclaimable_bytes == -1:
            # unknown, skip actual vacuum to be safe
            vacuum_reason = "dry_run_unknown"
        else:
            reclaimable_pct = reclaimable_bytes / total_size if total_size > 0 else 0.0
            if reclaimable_pct >= vacuum_reclaim_pct:
                # proceed to vacuum
                try:
                    run_vacuum(table_path, vacuum_retain_hours)
                    vacuum_executed = True
                    vacuum_reason = f"reclaimable_pct {reclaimable_pct:.4f} >= {vacuum_reclaim_pct}"
                except Exception as e:
                    print(f"[{now_iso()}] ERROR during VACUUM: {e}")
                    traceback.print_exc()
            else:
                vacuum_reason = f"reclaimable_pct {reclaimable_pct:.4f} < {vacuum_reclaim_pct} threshold"
    else:
        vacuum_reason = f"last_vacuum_recent ({last_vac_days} days) - skipping"

    # Write audit row
    after_metrics = {
        "numFiles": detail_after.get("numFiles"),
        "sizeInBytes": detail_after.get("sizeInBytes"),
        "avgFileMB": round((detail_after.get("sizeInBytes") / detail_after.get("numFiles") / (1024*1024)) if detail_after.get("numFiles") else 0, 2)
    }

    audit_row = {
        "run_time": datetime.datetime.utcnow(),
        "table_path": table_path,
        "action": "MAINTENANCE_RUN",
        "reason": json.dumps({
            "optimize_reason": optimize_reason,
            "vacuum_reason": vacuum_reason
        }),
        "before_metrics": before_metrics,
        "after_metrics": after_metrics,
        "executed_by": executed_by,
        "optimize_executed": optimize_executed,
        "vacuum_executed": vacuum_executed,
        "reclaimable_bytes": reclaimable_bytes if reclaimable_bytes is not None else -1,
        "total_size_bytes": total_size,
        "notes": ""
    }
    write_audit_row(audit_table, audit_row)
    print(f"[{now_iso()}] Maintenance complete for {table_path}. Audit written to {audit_table}")

