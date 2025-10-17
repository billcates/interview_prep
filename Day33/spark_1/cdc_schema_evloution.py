from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from delta.tables import DeltaTable
import datetime


TARGET_TABLE = "transactions"            # managed Delta table name
AUDIT_TABLE = "delta.schema_audit"       # schema audit table name
PK = "id"

spark = (
    SparkSession.builder
    .appName("DynamicSchemaEvolutionAndMerge")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

def spark_type_to_ddl(dtype):
    t = dtype.simpleString()
    if t.startswith("string"):
        return "STRING"
    if t.startswith("integer") or t.startswith("int"):
        return "INT"
    if t.startswith("double") or t.startswith("float"):
        return "DOUBLE"
    if t.startswith("long"):
        return "BIGINT"
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    # fallback
    return "STRING"

def default_for_type(dtype):
    t = dtype.simpleString()
    if t.startswith("string"):
        return "'UNKNOWN'"
    if t.startswith("integer") or t.startswith("int") or t.startswith("long"):
        return "-1"
    if t.startswith("double") or t.startswith("float"):
        return "-1.0"
    if t.startswith("timestamp"):
        return "current_timestamp()"
    return "NULL"

# ----------------------------
# 1) Create initial target Delta table with id, name, age
# ----------------------------
spark.sql(f"""
CREATE TABLE {TARGET_TABLE} (
  id STRING,
  name STRING,
  age INT
) USING DELTA
""")

print("Created initial target table with columns: id, name, age")

# Insert some base rows
base_rows = [
    ("101", "Alice", 30),
    ("102", "Bob", 25),
    ("103", "Carol", 40)
]
df_base = spark.createDataFrame(base_rows, schema=["id","name","age"])
df_base.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
print("Inserted base rows into target table.")

# Create empty audit table
spark.sql(f"""
CREATE TABLE {AUDIT_TABLE} (
  column_name STRING,
  action STRING,
  added_by STRING,
  added_at TIMESTAMP
) USING DELTA
""")

# Utility: get current target schema fields as dict name->StructField
def get_target_schema_fields(table_name):
    sch = spark.table(table_name).schema
    return {f.name: f for f in sch.fields}


def evolve_and_merge(source_df, source_name="source_batch"):
    """
    - Adds missing columns to target
    - Updates existing rows to fill defaults for new columns
    - Performs MERGE:
        WHEN MATCHED -> update columns present in source
        WHEN NOT MATCHED -> insert all source columns (plus nulls for missing)
    - Logs schema changes to audit table
    """
    target = TARGET_TABLE
    audit_table = AUDIT_TABLE

    # get current target schema fields
    target_fields = get_target_schema_fields(target)
    target_cols = set(target_fields.keys())

    # get source schema fields
    source_schema = source_df.schema
    source_fields = {f.name: f for f in source_schema.fields}
    source_cols = set(source_fields.keys())

    # Determine new columns to add to target
    new_cols = [c for c in source_cols if c not in target_cols]
    if new_cols:
        # Build ALTER TABLE ADD COLUMNS ddl fragment
        ddl_parts = []
        for nc in new_cols:
            dtype = source_fields[nc].dataType
            ddl_type = spark_type_to_ddl(source_fields[nc].dataType)
            ddl_parts.append(f"{nc} {ddl_type}")
        ddl_str = ", ".join(ddl_parts)
        alter_sql = f"ALTER TABLE {target} ADD COLUMNS ({ddl_str})"
        spark.sql(alter_sql)

        # After adding columns, write audit entries
        now = datetime.datetime.utcnow().isoformat()
        audit_rows = [(nc, "added_to_target", source_name, datetime.datetime.utcnow()) for nc in new_cols]
        df_audit = spark.createDataFrame(audit_rows, schema=["column_name","action","added_by","added_at"])
        df_audit.write.format("delta").mode("append").saveAsTable(audit_table)
        print("Logged schema changes to audit table:", new_cols)

        # Fill defaults for existing rows for each new column
        dt = DeltaTable.forName(spark, target)
        for nc in new_cols:
            dtype = source_fields[nc].dataType
            default_sql = default_for_type(dtype)
            dt.update(condition=f"{nc} IS NULL", set={nc: default_sql})

        target_fields = get_target_schema_fields(target)
        target_cols = set(target_fields.keys())

    dt = DeltaTable.forName(spark, target)
    # ensure source has columns for all target columns (if not present, add null columns)
    # Create a source_prepared DataFrame that has all target columns
    missing_in_source = [c for c in target_cols if c not in source_cols]
    # for missing columns, add a null column with same name
    src = source_df
    for c in missing_in_source:
        src = src.withColumn(c, F.lit(None).cast(target_fields[c].dataType))

  
    set_mapping = {}
    for c in target_cols:
        if c == PK:
            continue  # don't update the primary key
        if c in source_cols:
            set_mapping[c] = F.expr(f"s.`{c}`")  # use source column
        else:
            set_mapping[c] = F.expr(f"t.`{c}`")  # keep existing target value

    # Perform MERGE: match on PK
    print("Executing MERGE into target table...")
    dt.alias("t").merge(
        src.alias("s"),
        f"t.{PK} = s.{PK}"
    ).whenMatchedUpdate(set=set_mapping) \
     .whenNotMatchedInsertAll() \
     .execute()


data_a = [
    ("101", "Alice", 31, "US"),   # existing id 101, age updated, new country
    ("104", "David", 28, "UK")    # new id
]
schema_a = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True)
])
df_a = spark.createDataFrame(data_a, schema=schema_a)

print("\n=== Applying Batch A (introduces 'country') ===")
evolve_and_merge(df_a, source_name="batch_A")

print("Target table after Batch A:")
spark.table(TARGET_TABLE).show(truncate=False)

print("Schema audit table entries:")
spark.table(AUDIT_TABLE).show(truncate=False)
