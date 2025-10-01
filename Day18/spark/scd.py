from pyspark.sql.window import Window
from pyspark.sql import functions as f


def scd1(incremental_df,gold_df):
    window=Window.partitionBy("user_id").orderBy(f.desc("updated_at"))

    incremental_df=incremental_df.withColumn("rn",f.row_number().over(window)).filter(f.col("rn")==1).drop("rn")

    new_records_df=incremental_df.join(gold_df,on="user_id",how="left_anti")

    updated_records_df=(incremental_df.alias("l").join(gold_df.alias("r"),(f.col("l.user_id")==f.col("r.user_id"))&(f.col("l.updated_at")>f.col("r.updated_at")))
                         .select("l.*"))

    existing_records_df=(incremental_df.alias("l").join(gold_df.alias("r"),(f.col("l.user_id")==f.col("r.user_id"))&(f.col("l.updated_at")<f.col("r.updated_at")))
                         .select("r.*"))
    
    unchanged_records_df = gold_df.join(incremental_df, on="user_id", how="left_anti")

    unified_df=new_records_df.union(updated_records_df).union(existing_records_df).union(unchanged_records_df)

    return unified_df


#full outer join approach
# 
def scd1_full_outer_join(incremental_df, gold_df):
    # Step 1: Deduplicate incremental
    window = Window.partitionBy("user_id").orderBy(f.desc("updated_at"))
    deduped_incremental = (
        incremental_df
        .withColumn("rn", f.row_number().over(window))
        .filter(f.col("rn") == 1)
        .drop("rn")
    )
    
    # Step 2 & 3: Full outer join + conditional selection
    result_df = (
        deduped_incremental.alias("inc")
        .join(gold_df.alias("gold"), on="user_id", how="outer")
        .select(
            f.coalesce(f.col("inc.user_id"), f.col("gold.user_id")).alias("user_id"),
            f.when(f.col("inc.updated_at").isNull(), f.col("gold.name"))
             .when(f.col("gold.updated_at").isNull(), f.col("inc.name"))
             .when(f.col("inc.updated_at") >= f.col("gold.updated_at"), f.col("inc.name"))
             .otherwise(f.col("gold.name")).alias("name"),
            f.when(f.col("inc.updated_at").isNull(), f.col("gold.email"))
             .when(f.col("gold.updated_at").isNull(), f.col("inc.email"))
             .when(f.col("inc.updated_at") >= f.col("gold.updated_at"), f.col("inc.email"))
             .otherwise(f.col("gold.email")).alias("email"),
            f.greatest(
                f.coalesce(f.col("inc.updated_at"), f.col("gold.updated_at")),
                f.coalesce(f.col("gold.updated_at"), f.col("inc.updated_at"))
            ).alias("updated_at")
        )
    )
    
    return result_df