from pyspark.sql.functions import expr
import dlt

@dlt.view(
    name="stg_view"
)
def load_source():
    return (spark.readStream.format("delta")
            .option("readChangeFeed","true")
            .option("startingVersion",19).table("dlt_tbl"))

dlt.create_streaming_table("gold_table")

dlt.create_auto_cdc_flows(
    source="stg_view",
    target="gold_table",
    keys=["primary_key"],
    sequence_by='ts_col',
    stored_as_scd_type=1,
    appy_as_deletes=expr("operation='DELETE'"),
    except_column_list=["_commit_version"]
)