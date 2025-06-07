# Databricks notebook source
# MAGIC %md
# MAGIC **Profit By Year**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Year, round(sum(Profit), 2) as Profit
# MAGIC from gold_profit_agg
# MAGIC group by Year
# MAGIC order by Year

# COMMAND ----------

# MAGIC %md
# MAGIC **Profit By Customer**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Customer_Name, round(sum(Profit), 2) as Profit
# MAGIC from gold_profit_agg
# MAGIC --where Customer_Name = 'Dorothy Wardle'
# MAGIC group by Customer_Name

# COMMAND ----------

# MAGIC %md
# MAGIC **Profit By Year and Customer**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Year,Customer_Name, round(sum(Profit), 2) as Profit
# MAGIC from gold_profit_agg
# MAGIC --where Customer_Name = 'Dorothy Wardle'
# MAGIC group by Year,Customer_Name
# MAGIC order by Year
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Profit By Year and Category**

# COMMAND ----------

# MAGIC %sql
# MAGIC Select Year,Category, round(sum(Profit), 2) as Profit
# MAGIC from gold_profit_agg
# MAGIC --where Category = 'Technology'
# MAGIC group by Year,Category
# MAGIC order by Year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Select count(*) from gold_orders
# MAGIC
# MAGIC Select * from gold_profit_agg