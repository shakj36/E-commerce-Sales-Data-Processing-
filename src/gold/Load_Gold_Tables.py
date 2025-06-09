# Databricks notebook source
# MAGIC %run /Users/shakj9090@gmail.com/src/silver/Load_Silver_Tables

# COMMAND ----------


enriched_ord_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_orders ZORDER BY (ROW_ID)
# MAGIC

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_profit_agg")

profit_agg_df = enriched_ord_df.withColumn("Year", F.year("Order_Date")) \
    .groupBy("Year", "Category", "Sub_Category", "Customer_Name") \
    .agg(F.sum("Profit").alias("Profit"))

profit_agg_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("Year").saveAsTable("gold_profit_agg")    