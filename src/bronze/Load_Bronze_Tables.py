# Databricks notebook source
# MAGIC %md
# MAGIC **Load Bronze Tables**

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


orders_schema = StructType([
   StructField("Customer ID", StringType(), True),
   StructField("Discount", DoubleType(), True),
   StructField("Order Date", StringType(), True),
   StructField("Order ID", StringType(), True),
   StructField("Price", DoubleType(), True),
   StructField("Product ID", StringType(), True),
   StructField("Profit", DoubleType(), True),
   StructField("Quantity", IntegerType(), True),
   StructField("Row ID", IntegerType(), True),
   StructField("Ship Date", StringType(), True),
   StructField("Ship Mode", StringType(), True)

])

products_schema = StructType([
   StructField("Product_ID", StringType(), True),
   StructField("Category", StringType(), True),
   StructField("Sub_Category", StringType(), True),
   StructField("Product_Name", StringType(), True)

])   

customer_schema = StructType([
   StructField("Customer_ID", StringType(), True),
   StructField("Customer_Name", StringType(), True),
   StructField("Email", StringType(), True),
   StructField("Phone", StringType(), True),
   StructField("Address", StringType(), True),
   StructField("Segment", StringType(), True),
   StructField("Country", StringType(), True),
   StructField("City", StringType(), True),
   StructField("State", StringType(), True),
   StructField("Postal_Code", StringType(), True),
   StructField("Region", StringType(), True)

])


def sanitize_cols(df):
  cleaned_cols = [col.replace(" ","_").replace("-","_").strip() for col in df.columns]
  return df.toDF(*cleaned_cols)


def load_file(filepath: str, spark : SparkSession):
    ext = filepath.split(".")[-1].lower()

    print(f"\n Processing : {filepath} (.{ext})" )

    try :
          if "Orders" in filepath:
            df = spark.read.schema(orders_schema).option("multiline", "true").json(filepath)
            target_table = "bronze_orders"

          elif "Products" in filepath:
            df = spark.read.schema(products_schema).option("header", "true").csv(filepath)  
            target_table = "bronze_products"


          elif "Customer" in filepath:
            df = spark.read.format("com.crealytics.spark.excel") \
                .schema(customer_schema) \
                .option("header","true") \
                .load(filepath)
            target_table = "bronze_customer"

          else:
              raise Exception("Unknown File type")


          df = sanitize_cols(df)

          print(f"Loaded {df.count()} rows")   

          df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
              .saveAsTable(target_table) 

          print(f"Written to table : {target_table}")    

          df.createOrReplaceTempView(target_table)
      
    except Exception as e:
      print(f"Error in processing {filepath}: {e}")



files = [
    "dbfs:/FileStore/tables/Customer.xlsx",
    "dbfs:/FileStore/tables/Orders.json",
    "dbfs:/FileStore/tables/Products.csv"
]    

for file in files:
   load_file(file,spark)


def load_bronze_tables(spark):
   print("Bronze Tables Loaded")  