from pyspark.sql.functions import col, round
from delta.tables import DeltaTable

def check_delta_table_health(table_path: str, ideal_min_mb=64, ideal_max_mb=1024):

    detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
    detail = detail_df.collect()[0]
    
    total_files = detail['numFiles']
    total_size_bytes = detail['sizeInBytes']
    partition_cols = detail['partitionColumns']
    
    avg_file_size_mb = (total_size_bytes / total_files) / (1024 * 1024) if total_files > 0 else 0
    
    print("\n=============== 📌 Table Summary ===============")
    print(f"Total Files              : {total_files}")
    print(f"Total Data Size          : {round(total_size_bytes / (1024 * 1024 * 1024), 2)} GB")
    print(f"Avg File Size            : {round(avg_file_size_mb, 2)} MB")
    print(f"Partition Columns        : {partition_cols if partition_cols else ' Not Partitioned'}")
    
    
    # File Size Optimization
    if avg_file_size_mb < ideal_min_mb:
        print(f"Too many small files detected — RECOMMEND: `OPTIMIZE ZORDER BY`")
    elif avg_file_size_mb > ideal_max_mb:
        print(f"Files are too large — may impact scan performance.")
    else:
        print(f" File size is optimal")
    
    # Partition Strategy
    if not partition_cols:
        print("Table is not partitioned — consider partitioning if table > 200GB.")
    else:
        print("Partitioning is set — check cardinality in further audit.")
    
    print("\n Health Check Completed.\n")
