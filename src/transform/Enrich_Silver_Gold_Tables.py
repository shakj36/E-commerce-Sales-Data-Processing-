# Databricks notebook source
# MAGIC %md
# MAGIC Run Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC This part wont be needed in cases of running all modules through main.py 
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %run /Users/shakj9090@gmail.com/src/bronze/Load_Bronze_Tables 

# COMMAND ----------

# MAGIC %md
# MAGIC Enrich Silver Tables

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

address_parts = F.split("Address", ",")
window_cust = Window.orderBy("Customer_ID")
window_prod = Window.orderBy("Product_ID")

def enrich_customer_data(df):
    return df \
      .withColumn("Customer_PK", F.row_number().over(window_cust)) \
      .withColumn("Customer_Name", F.trim(F.regexp_replace("Customer_Name", r"[^\w\s]", ""))) \
      .withColumn("Address_Line1",F.trim(address_parts.getItem(0))) \
      .withColumn("Address_Line2",F.trim(address_parts.getItem(1))) \
      .withColumn("Customer_Name_Bad", F.regexp_extract(F.col("Customer_Name"), r"[^a-zA-Z\s{2,}]", 0)) \
      .withColumn("Phone_Bad", F.col("Phone").rlike(r"^\\-") | F.lower(F.col("Phone")).contains('#error')) \
      .select(
        "Customer_PK", "Customer_ID", "Customer_Name","Email","Phone", "Address_Line1","Address_Line2","Segment","Country","City","State","Postal_Code","Region","Customer_Name_Bad","Phone_Bad"
      )    


def enrich_product_data(df):
    return df \
      .withColumn("Product_PK", F.row_number().over(window_prod)) \
      .select(
           "Product_PK","Product_ID","Category","Sub_Category","Product_Name"
      )  

def enrich_order_data(orders_df, enriched_customers_df, enriched_products_df):
    refined_orders = orders_df \
      .withColumn("Profit", F.round(F.col("Profit"), 2)) \
      .withColumn("Order_Date", F.to_date(F.col("Order_Date"), "d/M/yyyy"))  \
      .withColumn("Ship_Date", F.to_date(F.col("Ship_Date"), "d/M/yyyy")) 

    enriched_orders = refined_orders \
      .join(enriched_customers_df, on = "Customer_ID", how ="inner") \
      .join(enriched_products_df, on = "Product_ID", how ="inner") \
      .select(
           "Row_ID","Order_ID","Order_Date","Ship_Date","Ship_Mode","Quantity","Price","Discount","Profit","Customer_Name","Country","Category","Sub_Category"
      )  

    return enriched_orders

      