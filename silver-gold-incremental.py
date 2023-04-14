# Databricks notebook source
import datetime
import pandas as pd
from pyspark.sql import SparkSession
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, from_utc_timestamp, current_timestamp,year, month, sum, current_date, date_sub, round, when, lag, col, max, min, desc

# COMMAND ----------

silver_df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
silver_df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")

# COMMAND ----------

from pyspark.sql.functions import year, month

# Assume you have a PySpark DataFrame named "df" with a date column named "date_column"

# Extract the year and month from the date column
df_mod_transactions = silver_df_transactions.withColumn('year', year(df_filtered_transactions['date']))
df_mod_transactions = df_mod_transactions.withColumn('month', month(df_filtered_transactions['date']))

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


# COMMAND ----------

# MAGIC %md
# MAGIC ## QUERY

# COMMAND ----------

gold_df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/gold/transactions/")

# COMMAND ----------

# Filter the DataFrame to include only transactions from the past three years
# Get the last day of the previous month
now = gold_df_transactions.agg(max('date')).collect()[0][0] #datetime.datetime.now() ## CHANGED TO MAX OF WHAT DATE IN THE TABLE
last_month = now.month - 1 if now.month > 1 else 12
last_month_year = now.year if now.month > 1 else now.year - 1
last_day_of_last_month = datetime.date(year = last_month_year, month = last_month,day =  1) + pd.offsets.MonthEnd(1) - datetime.timedelta(days=1)

# Get the last day of the current month
last_day_of_this_month = datetime.date(now.year, now.month, 1) + pd.offsets.MonthEnd(1) - datetime.timedelta(days=1)
print(last_day_of_this_month)
print(last_day_of_last_month)
print(now.day)
# Filter the DataFrame to only include rows where the date column is from a completed month
if now.day == last_day_of_this_month.day:
    df_filtered_transactions = gold_df_transactions[gold_df_transactions['date'] <= last_day_of_this_month]
else:
    df_filtered_transactions = gold_df_transactions[gold_df_transactions['date'] <= last_day_of_last_month]

# # Print the filtered DataFrame
df_filtered_transactions.orderBy(desc('date')).display()

# COMMAND ----------

df_filtered_transactions.agg(max('date')).collect()[0][0]
print(max_date)

# COMMAND ----------


# Step 4: Group by product_id, year, and month to calculate total sales
sales = (
    df_filtered_transactions.withColumn(
        "adjusted_signed_price",
        when(
            (col("transaction_type") == "return") & col("previous_purchase_signed_price").isNull(),
            col("daily_signed_price"),
        ).otherwise(col("daily_signed_price") + col("previous_purchase_signed_price")),
    )
    .groupBy("product_id", "year", "month")
    .agg(round(sum("daily_signed_price"), 2).alias("total_sales"))
    )

# COMMAND ----------

sales.show()

# COMMAND ----------

# sales.select("product_id", "year", "aliased_month", "total_sales","transaction_type").display()

# COMMAND ----------

sorted_sales = sales.orderBy('year','month')
sorted_sales.show()

# COMMAND ----------

# Calculate the month over month sales by amount
sorted_sales = sorted_sales.withColumn('MoM_growth', col('total_sales') - lag('total_sales', 1).over(Window.partitionBy('product_id').orderBy('year', 'month')))
sorted_sales = sorted_sales.withColumn('MoM_pct_growth', round((col('MoM_growth')/lag('total_sales', 1).over(Window.partitionBy('product_id').orderBy('year', 'month')))*100, 2))

# Add a special case for the first month
sorted_sales = sorted_sales.withColumn('MoM_growth', when(col('MoM_growth').isNull(), col('total_sales')).otherwise(col('MoM_growth')))
sorted_sales = sorted_sales.withColumn('MoM_pct_growth', when(col('MoM_pct_growth').isNull(), 0.0).otherwise(col('MoM_pct_growth')))

# COMMAND ----------

sorted_sales.filter(sorted_sales.product_id == 'product0').display()

# COMMAND ----------


