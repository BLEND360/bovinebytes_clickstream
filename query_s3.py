# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## BOVINE BYTES
# MAGIC ### MONTH OVER MONTH SALES REPORT ON BLEND360 TUMBLERS FOR THE LAST 3 YEARS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## INGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### LOAD DATA INTO S3 USING PYTHON

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### DISPLAY BRONZE DIRECTORY

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/test

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/bronze

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/bronze/clickstream

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/bronze/transactions

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/bronze/products

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/bronze/users

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ BRONZE TABLES INTO DATAFRAME

# COMMAND ----------

# Load four tables from Amazon S3 into Spark DataFrames
# Load the 'users' table from the 'bronze' directory
df_users = spark.read.format("delta").load("s3://allstar-training-bovinebytes/bronze/users/")

# Load the 'products' table from the 'bronze' directory
df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/bronze/products/")

# Load the 'clickstream' table from the 'bronze' directory and enable recursive file lookup
df_clickstream = spark.read.option("recursiveFileLookup", "true").parquet("s3://allstar-training-bovinebytes/bronze/clickstream/")

# Load the 'transactions' table from the 'bronze' directory and enable recursive file lookup
df_transactions = spark.read.option("recursiveFileLookup", "true").parquet("s3://allstar-training-bovinebytes/bronze/transactions/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### VALIDATE SOURCE DATA
# MAGIC <ul>
# MAGIC   <li>Row Count</li>
# MAGIC   <li>Dups Check</li>
# MAGIC   <li>Get Min and Max Date</li>
# MAGIC <ul>

# COMMAND ----------

#Users Count
df_users.count()

# COMMAND ----------

#Products Count
df_products.count()

# COMMAND ----------

#Clickstream Count
df_clickstream.count()

# COMMAND ----------

#Transaction Count
df_transactions.count()

# COMMAND ----------

# Find the minimum and maximum values of the 'utc_date' column in the 'transactions' DataFrame
# Calculate the minimum value using the 'agg()' method to aggregate the minimum value
min_val = df_transactions.agg({"utc_date": "min"}).collect()[0][0]
# Calculate the maximum value using the 'agg()' method to aggregate the maximum value
max_val = df_transactions.agg({"utc_date": "max"}).collect()[0][0]

# Print the minimum and maximum values of the 'utc_date' column
print(min_val)
print(max_val)

# COMMAND ----------

# Find the minimum and maximum values of the 'utc_date' column in the 'clickstream' DataFrame
# Calculate the minimum value using the 'agg()' method to aggregate the minimum value
min_val = df_clickstream.agg({"utc_date": "min"}).collect()[0][0]
# Calculate the maximum value using the 'agg()' method to aggregate the maximum value
max_val = df_clickstream.agg({"utc_date": "max"}).collect()[0][0]

# Print the minimum and maximum values of the 'utc_date' column
print(min_val)
print(max_val)

# COMMAND ----------

from pyspark.sql.functions import count, desc

# Group the 'transactions' DataFrame by email address and count the number of transactions for each email address
grouped_df = df_transactions.groupBy("email").agg(count("*").alias("count"))

# Filter the results to only show email addresses that have more than one transaction
filtered_df = grouped_df.filter("count > 1")

# Order the results by the count in descending order
ordered_df = filtered_df.orderBy(desc("count"))

# Display the results in a table format
ordered_df.display()


# COMMAND ----------

# Filter the 'transactions' DataFrame to only keep transactions where the email address is "805ad97@gmail.com"
filtered_df = df_transactions.where(df_transactions.email == "805ad97@gmail.com")

# Sort the filtered transactions by their 'transaction_timestamp' column in ascending order
sorted_df = filtered_df.orderBy("transaction_timestamp")

# Display the sorted transactions in a table format
sorted_df.display()

# COMMAND ----------

from pyspark.sql.functions import date_trunc

df_transactions = df_transactions.withColumn("date", date_trunc("day", "utc_date"))

df_transactions.write.partitionBy("date").parquet("s3://allstar-training-bovinebytes/test_partition/transactions/")

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/test_partition/transactions

# COMMAND ----------

df_transactions_partitioned = spark.read.parquet("s3://allstar-training-bovinebytes/test_partition/transactions/")


# COMMAND ----------

from pyspark.sql.functions import max
max_date = df_transactions_partitioned.agg(max("date")).collect()[0][0]
print(max_date)

start_date = max_date + 1
end_date = today - 1
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CREATING USERS SILVER TABLE

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, from_utc_timestamp, current_timestamp, date_trunc
from pyspark.sql.functions import max

# COMMAND ----------

# Add new columns to the 'df_users' DataFrame to convert the Unix timestamp and add the ETL loaded timestamp
silver_df_users_tmp = df_users.withColumn("first_timestamp", from_unixtime("first_time_time_stamp"))
silver_df_users_tmp = silver_df_users_tmp.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

# silver_df_users_tmp = silver_df_users_tmp.withColumn("date", date_trunc("day", "first_timestamp"))

# Select only the required columns for the new silver table
silver_df_users_tmp = silver_df_users_tmp.select("account_id", "email", "first_timestamp", "etl_loaded_at")

silver_df_users_tmp.write.format("delta").mode("overwrite").save("s3://allstar-training-bovinebytes/silver/users/")

# Write the new DataFrame to a parquet file in Amazon S3
# silver_df_users_tmp.write.mode("append").parquet("s3://allstar-training-bovinebytes/silver/users/")


# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/silver/users

# COMMAND ----------

# Read parquet files from Amazon S3 for silver users table
silver_df_users = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/users/")
#spark.read.option("recursiveFileLookup", "true").parquet("s3://allstar-training-bovinebytes/silver/users/")

# Display the contents of the table in a table format
silver_df_users.display()

# COMMAND ----------

silver_df_users.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CREATING PRODUCTS SILVER TABLE

# COMMAND ----------

# Add a new column to the 'df_products' DataFrame to add the ETL loaded timestamp
silver_df_products_tmp = df_products.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

# Write the new DataFrame to a parquet file in Amazon S3
silver_df_products_tmp.write.format("delta").mode("overwrite").save("s3://allstar-training-bovinebytes/silver/products/")

# COMMAND ----------

# Read parquet files from Amazon S3 for silver products table
silver_df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")

# Display the contents of the table in a table format
silver_df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CREATING TRANSACTIONS SILVER TABLE

# COMMAND ----------

# Create a new DataFrame that combines information from 'df_transactions' and 'df_products'

silver_df_transactions_get_max = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
max_date = silver_df_transactions_get_max.agg(max("date")).collect()[0][0]

silver_df_transactions_tmp = df_transactions.selectExpr("*", "explode(items) as product_id").filter(df_transactions.utc_date > max_date)

if silver_df_transactions_tmp.count() > 0:    
    silver_df_transactions_tmp = silver_df_transactions_tmp.join(df_products, "product_id")

    # Add new columns to the 'silver_df_transactions_tmp' DataFrame to convert the Unix timestamp and add the ETL loaded timestamp
    silver_df_transactions_tmp = silver_df_transactions_tmp.withColumn("transaction_utc_timestamp", from_unixtime("transaction_timestamp"))
    silver_df_transactions_tmp = silver_df_transactions_tmp.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

    silver_df_transactions_tmp = silver_df_transactions_tmp.withColumn("date", date_trunc("day", "utc_date"))

    # Select only the required columns for the new silver table
    silver_df_transactions_tmp = silver_df_transactions_tmp.select("order_id", "email", "transaction_utc_timestamp", "transaction_type", "product_name", "product_id", "price", "etl_loaded_at", "date")

    # Write the new DataFrame to a partquet file in Amazon S3
    silver_df_transactions_tmp.write.format("delta").partitionBy("date").mode("append").save("s3://allstar-training-bovinebytes/silver/transactions")

# COMMAND ----------

# MAGIC %fs ls s3://allstar-training-bovinebytes/silver/transactions

# COMMAND ----------

# Read parquet files from Amazon S3 for silver transaction table
silver_df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")

# Display the contents of the table in a table format
silver_df_transactions.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### CREATING CLICKSTREAM SILVER TABLE

# COMMAND ----------

silver_df_clickstream_get_max = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/clickstream/")
max_date = silver_df_clickstream_get_max.agg(max("date")).collect()[0][0]

# Add new columns to the 'df_clickstream' DataFrame to convert Unix time to standard timestamp format
silver_df_clickstream_tmp = df_clickstream.withColumn("cust_hit_gmt_timestamp", from_unixtime("cust_hit_time_gmt")).filter(df_clickstream.utc_date > max_date)

if silver_df_clickstream_tmp.count() > 0:
    silver_df_clickstream_tmp = silver_df_clickstream_tmp.withColumn("first_hit_gmt_timestamp", from_unixtime("first_hit_time_gmt"))
    silver_df_clickstream_tmp = silver_df_clickstream_tmp.withColumn("hit_gmt_timestamp", from_unixtime("hit_time_gmt"))
    silver_df_clickstream_tmp = silver_df_clickstream_tmp.withColumn("visit_start_gmt_timestamp", from_unixtime("visit_start_time_gmt"))
    silver_df_clickstream_tmp = silver_df_clickstream_tmp.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

    silver_df_clickstream_tmp = silver_df_clickstream_tmp.withColumn("date", date_trunc("day", "utc_date"))


    # Select only the required columns for the new Delta table
    silver_df_clickstream_tmp = silver_df_clickstream_tmp.select("campaign", "currency", "cust_hit_gmt_timestamp", "cust_visid", "event_list", "first_hit_gmt_timestamp", "hit_gmt_timestamp", "hitid_high", "hitid_low", "os", "page_url", "pagename", "product_list", "visid_high", "visid_low", "visit_start_gmt_timestamp", "evar1", "evar2", "evar3", "evar4", "evar5", "evar6", "etl_loaded_at", "date")

    # Write the new DataFrame to a partquet file in Amazon S3
    silver_df_clickstream_tmp.write.format("delta").partitionBy("date").mode("append").save("s3://allstar-training-bovinebytes/silver/clickstream")
else:
    print("No new data")

# COMMAND ----------

# Read parquet files from Amazon S3
silver_df_clickstream = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/clickstream/")

# Display the contents of the table in a table format
silver_df_clickstream.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## CREATING REPORT

# COMMAND ----------

from pyspark.sql.functions import year, month, sum, current_date, date_sub, round, when

# Extract year and month from the 'transaction_utc_timestamp' column
silver_df_transactions_yearflt = silver_df_transactions.withColumn("year", year("transaction_utc_timestamp")).withColumn("month", month("transaction_utc_timestamp"))

# Filter the DataFrame to include only transactions from the past three years
today = current_date()
silver_df_transactions_yearflt = silver_df_transactions_yearflt.filter((year("transaction_utc_timestamp") >= year(date_sub(today, 1095))) & (silver_df_transactions_yearflt.transaction_type=='purchase'))

# Group the DataFrame by product, year, and month and calculate total sales
sales = silver_df_transactions_yearflt.groupBy("product_id", "year", "month").agg(round(sum("price"), 2).alias("total_sales"))

# Filter the DataFrame to only include data for a specific product
sales = sales.filter(sales["product_id"] == "product0").orderBy("year", "month")

# Add a column to the DataFrame showing the product name and the month as a string abbreviation
sales = sales.withColumn("product_name", when(sales.product_id == "product0", "Tumbler")) \
    .withColumn("aliased_month", when(sales.month == "1", "Jan") \
    .when(sales.month == "2", "Feb") \
    .when(sales.month == "3", "Mar") \
    .when(sales.month == "4", "Apr") \
    .when(sales.month == "5", "May") \
    .when(sales.month == "6", "Jun") \
    .when(sales.month == "7", "Jul") \
    .when(sales.month == "8", "Aug") \
    .when(sales.month == "9", "Sep") \
    .when(sales.month == "10", "Oct") \
    .when(sales.month == "11", "Nov") \
    .when(sales.month == "12", "Dec"))

# Select only the required columns and display the resulting DataFrame
sales.select("product_name", "year", "aliased_month", "total_sales").display()


# COMMAND ----------

dbutils.fs.rm("s3://allstar-training-bovinebytes/silver/transactions", recurse=True)

# COMMAND ----------


