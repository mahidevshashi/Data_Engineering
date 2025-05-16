# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation Data from Bronze Layer
# MAGIC
# MAGIC # Load data from Bronze Layer
# MAGIC bronze_df = spark.read.format("delta").load("/mnt/bronze/data")
# MAGIC
# MAGIC # Perform transformations
# MAGIC transformed_df = bronze_df.withColumnRenamed("old_column_name", "new_column_name") \
# MAGIC                           .filter(bronze_df["some_column"] > 100) \
# MAGIC                           .drop("unnecessary_column")
# MAGIC
# MAGIC # Display the transformed data
# MAGIC display(transformed_df)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### loading data

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.azurestoragesta.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azurestoragesta.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azurestoragesta.dfs.core.windows.net", "1f1c4a5b-97df-48c9-9c21-0cd0e46c911f")
spark.conf.set("fs.azure.account.oauth2.client.secret.azurestoragesta.dfs.core.windows.net", "DBL8Q~n4fRwnXAYGUTUgm~A2.yKQbWweHfKCHdlC")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azurestoragesta.dfs.core.windows.net", "https://login.microsoftonline.com/7749dd34-ffcf-4b56-9131-61e2702dceeb/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Calender Data

# COMMAND ----------

bronze_df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/calenders/calenders.csv')

# COMMAND ----------



# COMMAND ----------

bronze_df.display()

# COMMAND ----------

bronze_cus = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/customers')
bronze_cus.display()

# COMMAND ----------

bronze_products = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/Products')

# COMMAND ----------

bronze_product_category = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/Product_Categories')

# COMMAND ----------

bronze_product_category.write.format('parquet')\
    .mode('overwrite')\
        .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Product_Categories')

# COMMAND ----------

bronze_product_subcategory = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

bronze_product_subcategory.write.format('parquet')\
    .mode('overwrite')\
        .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

bronze_return = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/Returns')

# COMMAND ----------

bronze_sales = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/Sales*')

# COMMAND ----------

bronze_terr = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('abfss://bronze@azurestoragesta.dfs.core.windows.net/Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC transformation

# COMMAND ----------

# MAGIC %md
# MAGIC after pushing it to the silver layer

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC adding new cloumn to calendar table

# COMMAND ----------

df_cal = bronze_df.withColumn('Month', month('Date'))\
                    .withColumn('Year', year('Date'))\
                        .withColumn('Week', weekofyear('Date'))\
                            .withColumn('Day', dayofmonth('Date'))\
                                .withColumn('DayofWeek', dayofweek('Date'))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Calender')

# COMMAND ----------

bronze_cus.display()

# COMMAND ----------

# MAGIC %md
# MAGIC creating new called full name which concatanating prefix, first name, last _name_

# COMMAND ----------

df_cus = bronze_cus.withColumn("fullname", concat(col('prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName')))
# bronze_cus.display()
df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('overwrite')\
        .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Customers')

# COMMAND ----------

bronze_products.display()
# bronze_products.write.format('parquet')\
#     .mode('overwrite')\
#         .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Products')

# COMMAND ----------

df_products = bronze_products.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
                                .withColumn('ProductName', split(col('ProductName'), '-')[0])
df_products.display()

# COMMAND ----------

df_products.write.format('parquet')\
    .mode('overwrite')\
        .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Products')

# COMMAND ----------

# bronze_return.display()
bronze_return.write.format('parquet')\
    .mode('overwrite')\
        .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Returns')
# bronze_sales.display()

# COMMAND ----------

bronze_terr.write.format('parquet')\
    .mode('overwrite')\
        .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Territory')

# COMMAND ----------

bronze_sales.display()

# COMMAND ----------

df_sales = bronze_sales.withColumn('StockDate', to_timestamp('StockDate'))\
                        .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))\
                            .withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))
                        
df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_Orders')).display()

# COMMAND ----------

df_sales.write.format('parquet')\
        .mode('overwrite')\
            .save('abfss://silver@azurestoragesta.dfs.core.windows.net/Sales')