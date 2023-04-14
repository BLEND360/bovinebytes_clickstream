# Databricks notebook source
import datetime
import pandas as pd
from pyspark.sql import SparkSession
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, from_utc_timestamp, current_timestamp,year, month, sum, current_date, date_sub, round, when, lag, col, max, min, desc

# COMMAND ----------

spark = SparkSession.builder.appName("myApp").getOrCreate()

silver_df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
silver_df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")

# COMMAND ----------

from pyspark.sql.functions import year, month

# Assume you have a PySpark DataFrame named "df" with a date column named "date_column"

# Extract the year and month from the date column
df_mod_transactions = silver_df_transactions.withColumn('year', year(silver_df_transactions['date']))
df_mod_transactions = df_mod_transactions.withColumn('month', month(df_mod_transactions['date']))

# COMMAND ----------


# Step 1: Create a signed_price column
df_mod_transactions = df_mod_transactions.withColumn(
    "signed_price",
    when(col("transaction_type") == "return", -col("price")).otherwise(col("price")),
)
# Step 2: Aggregate by email, product_id, and transaction_date
df_grouped_transactions = df_mod_transactions.groupBy(
    "email", "product_id", "date","year","month","transaction_type"
).agg(sum("signed_price").alias("daily_signed_price"))

# Step 3: Use lag function to find previous purchase date for each returned product
window_spec = Window.partitionBy("email", "product_id").orderBy("date")

df_grouped_transactions = df_grouped_transactions.withColumn(
    "previous_purchase_signed_price",
    lag("daily_signed_price").over(window_spec),
)

df_grouped_transactions.write.format("delta").partitionBy("date").mode("append").save("s3://allstar-training-bovinebytes/gold/transactions")


