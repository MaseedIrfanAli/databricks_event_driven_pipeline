# Databricks notebook source
from delta.tables import *
stage_table_name = "incremental_load.default.orders_stage"
target_table_name = "incremental_load.default.orders_target"


# COMMAND ----------

stage_df = spark.read.table(stage_table_name)

# COMMAND ----------

# Create equivalent target table schema if target table doesn't exist

if not spark._jsparkSession.catalog().tableExists(target_table_name):
    stage_df.write.format("delta").saveAsTable(target_table_name)
    
else:
    # Perform delta table merge query for upsert based on tracking_num column
    target_table = DeltaTable.forName(spark, target_table_name)

    # Define the merge condition based on the tracking_num column
    merge_condition = "stage.tracking_num = target.tracking_num"

    # Execute the merge operation
    target_table.alias("target") \
        .merge(stage_df.alias("stage"), merge_condition) \
        .whenMatchedDelete() \
        .execute()

    stage_df.write.format("delta").mode("append").saveAsTable(target_table_name)

# COMMAND ----------


