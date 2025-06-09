# Databricks notebook source
dbutils.widgets.text("bronze_customer_path", "dbfs:/user/hive/warehouse/bronze_customer")
dbutils.widgets.text("bronze_product_path", "dbfs:/user/hive/warehouse/bronze_products")
dbutils.widgets.text("bronze_order_path", "dbfs:/user/hive/warehouse/bronze_orders")

dbutils.widgets.text("silver_customer_table", "silver_customer")
dbutils.widgets.text("bad_silver_customer_table", "bad_silver_customer")
dbutils.widgets.text("silver_product_table", "silver_products")

# COMMAND ----------

# bronze_customer_path = dbutils.widgets.get("bronze_customer_path")
# bronze_product_path = dbutils.widgets.get("bronze_product_path")
# bronze_order_path = dbutils.widgets.get("bronze_order_path")

# silver_customer_table = dbutils.widgets.get("silver_customer_table")
# bad_silver_customer_table = dbutils.widgets.get("bad_silver_customer_table")
# silver_product_table = dbutils.widgets.get("silver_product_table")

# COMMAND ----------

# print("Customer path is :", bronze_customer_path )

# display(dbutils.fs.ls(bronze_customer_path))

# COMMAND ----------

with open("/tmp/requirements.txt", "w") as f :
    f.write("""
pytest>=7.4
typing_extensions>=4.5
pytest-mock
coverage
""".strip())

# COMMAND ----------

# MAGIC %pip install -r /tmp/requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run /Users/shakj9090@gmail.com/src/transform/Enrich_Silver_Gold_Tables

# COMMAND ----------

import sys
sys.path.append("Workspace/Users/shakj9090@gmail.com/src")

# COMMAND ----------

conftest_code = """
import sys
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope = "session")
def spark():
    spark = getattr(sys.modules["__main__"], "spark", None)

    if spark is None:
        try:
           spark = SparkSession.getActiveSession()
        except Exception:
           spark = None


    if spark is None :
       spark = SparkSession.builder.master("local[*]").appName("pytest-context").getOrCreate()    

    return spark
    
@pytest.fixture
def bronze_customer_df(spark):
    return spark.read.format("delta").load("/user/hive/warehouse/bronze_customer")

@pytest.fixture
def bronze_product_df(spark):
    return spark.read.format("delta").load("/user/hive/warehouse/bronze_products")

@pytest.fixture
def bronze_orders_df(spark):
    return spark.read.format("delta").load("/user/hive/warehouse/bronze_orders")      

"""

with open("/databricks/driver/conftest.py", "w") as f:
    f.write(conftest_code)


# COMMAND ----------

# MAGIC %md
# MAGIC **TESTING & VALIDATING DATA**

# COMMAND ----------

test_code ="""
import pytest
import logging
from pyspark.sql import Row
from conftest import spark,bronze_customer_df,bronze_product_df,bronze_orders_df
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

logger = logging.getLogger(__name__) 



def test_col_trans_customer(bronze_customer_df):
    from __main__ import enrich_customer_data
    enriched_df = enrich_customer_data(bronze_customer_df)
    expected_columns = {
        "Customer_PK", "Customer_ID", "Customer_Name","Email","Phone", "Address_Line1","Address_Line2","Segment","Country","City","State","Postal_Code","Region","Customer_Name_Bad","Phone_Bad"
    }   
    assert set(enriched_df.columns) == expected_columns




def test_validate_bad_records(bronze_customer_df):
    from __main__ import enrich_customer_data
    enriched_df = enrich_customer_data(bronze_customer_df)
    bad_df = enriched_df.filter(
        (F.col("Customer_Name_Bad") != "") | (F.col("Phone_Bad") == True) 
    )

    bad_rows = bad_df.collect()

    assert all(r.Customer_Name_Bad or r.Phone_Bad for r in bad_rows), "Invalid records not detected"

    for row in bad_rows:
        logger.warning(f"Bad Record : ID ={row.Customer_ID}, Name = {row.Customer_Name}, Phone = {row.Phone}" )




def test_col_trans_prod(bronze_product_df):
    from __main__ import enrich_product_data
    enriched_prod_df = enrich_product_data(bronze_product_df)
    expected_columns = {"Product_PK","Product_ID","Category","Sub_Category","Product_Name"}
    assert set(enriched_prod_df.columns) == expected_columns



def test_low_profit_customer(bronze_orders_df,bronze_customer_df,bronze_product_df):
    from __main__ import enrich_order_data,enrich_customer_data,enrich_product_data

    enriched_cust_df = enrich_customer_data(bronze_customer_df)
    enriched_prod_df = enrich_product_data(bronze_product_df)   

    enriched_cust_filter_df = enriched_cust_df.filter( (F.col("Customer_Name_Bad") == "") & (F.col("Phone_Bad") == False) ) \
    .select("Customer_PK", "Customer_ID", "Customer_Name","Email","Phone", "Address_Line1","Address_Line2","Segment","Country","City","State","Postal_Code","Region")

    enriched_orders_df = enrich_order_data(bronze_orders_df,enriched_cust_filter_df,enriched_prod_df)

    low_profit_df = enriched_orders_df.withColumn("Profit_Percent", (F.col("Profit") / F.col("Price")) * 100 ) \
                    .filter(F.col("Profit_Percent") < 10)

    low_profit_rows = low_profit_df.select("Customer_Name","Profit_Percent").collect()

    assert all(row.Profit_Percent < 10 for row in low_profit_rows), "Some Customers have profit > 10%"

    for row in low_profit_rows:
         logger.warning(f"Customer: {row.Customer_Name}, Profit_Percent : {row.Profit_Percent}" )




@pytest.mark.parametrize("missing_col", ["Customer_ID","Address_Line1","Phone"])
def test_empty_customer_missing_col(spark, missing_col):
    from __main__ import enrich_customer_data
    import __main__
    
    full_cols = [
        "Customer_PK", "Customer_ID", "Customer_Name","Email","Phone", "Address_Line1","Address_Line2","Segment","Country","City","State","Postal_Code","Region","Customer_Name_Bad","Phone_Bad"
    ]

    test_cols = [c for c in full_cols if c != missing_col]

    schema = StructType([StructField(c, StringType(), True) for c in test_cols])

    malformed_df = spark.createDataFrame([], schema = schema)

    with pytest.raises(Exception) as excinfo :
        enrich_customer_data(malformed_df)
    
    logger.warning(f"Expected failure due to Missing col: {excinfo.value}")   
    assert missing_col in str(excinfo.value) or "cannot resolve" in str(excinfo.value)
         



@pytest.mark.usefixtures("spark")
def test_prod_enrichment_invalid_types(bronze_product_df):
    from __main__ import enrich_product_data

    expected_schema = {"Product_ID" : StringType,"Category": StringType,"Sub_Category": StringType,"Product_Name": StringType}

    malformed_df = bronze_product_df.select(
        F.col("Product_ID").cast("int"),
        F.col("Category").cast("int"),
        F.col("Sub_Category").cast("int"),
        F.col("Product_Name").cast("int")

    )   

    mismatched_cols = []

    for col,expected_type in expected_schema.items():
        if col in malformed_df.columns:
           actual_type = malformed_df.schema[col].dataType
           if not isinstance(actual_type, expected_type):
              logger.warning(
                  f"Column '{col}' has type '{actual_type.simpleString()}', expected '{expected_type.__name__}'"
              )
              mismatched_cols.append(col, actual_type.simpleString())

    assert not mismatched_cols, f"Type mismatch in columns: {mismatched_cols}"

    enrich_product_data(malformed_df)


"""


with open("/databricks/driver/test_enrich.py", "w") as f:
    f.write(test_code)  

# COMMAND ----------

#import os
import pytest

result = pytest.main(["/databricks/driver/test_enrich.py", "-v", "-s"])

# COMMAND ----------

from pyspark.sql import functions as F

bronze_cust_df = spark.read.format("delta").load("/user/hive/warehouse/bronze_customer")
bronze_prod_df = spark.read.format("delta").load("/user/hive/warehouse/bronze_products")
bronze_order_df = spark.read.format("delta").load("/user/hive/warehouse/bronze_orders")   

# print("No of partitions:" , bronze_order_df.rdd.getNumPartitions())

enriched_cust_df = enrich_customer_data(bronze_cust_df)
enriched_prod_df = enrich_product_data(bronze_prod_df)

# print("No of partitions:" , enriched_cust_df.rdd.getNumPartitions())
# print("No of partitions:" , enriched_prod_df.rdd.getNumPartitions())

valid_cust_df = enriched_cust_df.filter( (F.col("Customer_Name_Bad") == "") & (F.col("Phone_Bad") == False) ) \
.select("Customer_PK", "Customer_ID", "Customer_Name","Email","Phone", "Address_Line1","Address_Line2","Segment","Country","City","State","Postal_Code","Region")

#Filtering the bad_cust_df for predicate push down approach
bad_cust_df = enriched_cust_df.filter( (F.col("Customer_Name_Bad") == True) | (F.col("Phone_Bad") == True) )

enriched_ord_df = enrich_order_data(bronze_order_df,valid_cust_df,enriched_prod_df)

enriched_ord_df.createOrReplaceGlobalTempView("enriched_ord_df")

#Caching enriched_ord_df to avoid repitive recomputation
enriched_ord_df.cache()

#To look into the behaviour of AQE 
# enriched_ord_df.explain(extended=True)

# display(enriched_ord_df)
# print("No of partitions:" , enriched_ord_df.rdd.getNumPartitions())

# COMMAND ----------

cleaned_df = valid_cust_df.drop("Customer_Name_Bad","Phone_Bad")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_customer")
spark.sql("DROP TABLE IF EXISTS bad_silver_customer")
spark.sql("DROP TABLE IF EXISTS gold_orders")


cleaned_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver_customer")
bad_cust_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("bad_silver_customer")
enriched_prod_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver_products")


# enriched_ord_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_orders")



# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS gold_profit_agg")

# profit_agg_df = enriched_ord_df.withColumn("Year", F.year("Order_Date")) \
#     .groupBy("Year", "Category", "Sub_Category", "Customer_Name") \
#     .agg(F.sum("Profit").alias("Profit"))

# profit_agg_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("Year").saveAsTable("gold_profit_agg")    
