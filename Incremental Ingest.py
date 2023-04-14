# Databricks notebook source
# MAGIC %pip install blend360_all_star_clickstream_api

# COMMAND ----------

# Import statements
import datetime
from blend360_all_star_clickstream_api.datafetch import DataFetch
import pandas as pd
from pyspark.sql import SparkSession
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, from_utc_timestamp, current_timestamp,year, month, sum, current_date, date_sub, round, when, lag, col

# COMMAND ----------

# Load the 'transactions' table from the 'bronze' directory and enable recursive file lookup
spark = SparkSession.builder.appName("myApp").getOrCreate()
df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")

# COMMAND ----------

df_transactions.show(5)

# COMMAND ----------

start_date = df_transactions.agg({"date": "max"}).collect()[0][0]
one_day = datetime.timedelta(days=1)
start_date = start_date + one_day
start_date = start_date.date()
print(start_date)

# COMMAND ----------

print(type(start_date))

# COMMAND ----------

end_date = datetime.datetime.now().date()
end_date = end_date - one_day
print(end_date)

# COMMAND ----------

data_fetch = DataFetch(secret_scope='my-scope',key_name='api-key')

def fetchData(start_date,end_date,destination_bucket, destination_directory, table_name):
    response = data_fetch.fetchData(start_date = start_date, end_date = end_date, destination_bucket = destination_bucket, destination_directory = destination_directory, table_name = table_name)
    if response.status_code == 200:
    # Get job status from response JSON
        data = response.json()
        jobID = data['job_id']
  
        print('Your JobID is ' + str(jobID) )
        print('You can use this to check your Job Status by going to the second option from the menu.')
        statusCheck(jobID)
        return jobID
    else:
        print("Error:", response.text)

# COMMAND ----------

def statusCheck(jobID):
    response = data_fetch.checkStatus(job_id = str(jobID))
    
    if response.status_code == 200:
        data = response.json()
        # Get job status from response JSON
        return data['execution_status']
    else:
        print("Error:", response.text)

# COMMAND ----------

def updateAPIKey():
    data_fetch.updateAPIKey()

# COMMAND ----------

def deleteAPIKey():
    data_fetch.deleteAPIKey()

# COMMAND ----------

tables = ['transactions','clickstream','products','users']
year = str(start_date.year)
dest_bucket = 'allstar-training-bovinebytes'
print('bronze/'+tables[0]+'/'+year)

# COMMAND ----------

tables_type1 = ['products','users']
tables_type2 = ['transactions','clickstream']
year = str(end_date.year)
dest_bucket = 'allstar-training-bovinebytes'

try:
    for each in tables_type2:
        print(each)
        flag = False
        
        dest_dir = str('bronze/'+str(each)+'/'+year)
        jobID = fetchData(start_date= start_date, end_date=end_date, destination_bucket= dest_bucket, destination_directory= dest_dir , table_name = each)
        
        while not flag:
            job_status = statusCheck(jobID) 
            print(job_status)
            if job_status == 'COMPLETE':
                flag = True
            else:
                # Wait for 5 seconds before trying again
                time.sleep(10)

    # Continue with the next iteration of the for loop
except Exception as e:
        print("Error:", str(e))
        


# COMMAND ----------

try:
    
    for each in tables_type1:
        print(each)
        flag = False
        
        dest_dir = str('bronze/'+str(each))
        jobID = fetchData(start_date= start_date, end_date=end_date, destination_bucket= dest_bucket, destination_directory= dest_dir , table_name = each)
        
        while not flag:
            job_status = statusCheck(jobID) 
            print(job_status)
            if job_status == 'COMPLETE':
                flag = True
            else:
                # Wait for 5 seconds before trying again
                time.sleep(10)

    # Continue with the next iteration of the for loop
except Exception as e:
        print("Error:", str(e))

# COMMAND ----------

df_transactions.show()

# COMMAND ----------

df_products.show()

# COMMAND ----------

# Add new columns to the 'silver_df_transactions_tmp' DataFrame to convert the Unix timestamp and add the ETL loaded timestamp
df_transactions = df_transactions.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

# Select only the required columns for the new silver table
df_transactions = df_transactions.select("order_id", "email", "transaction_utc_timestamp", "transaction_type", "product_name", "product_id", "price", "etl_loaded_at")

# Write the new DataFrame to a partquet file in Amazon S3
df_transactions.write.mode("append").parquet("s3://allstar-training-bovinebytes/silver/transactions/2023")

df_transactions = df_transactions.withColumn("year", year("transaction_utc_timestamp")).withColumn("month", month("transaction_utc_timestamp"))

# Filter the DataFrame to include only transactions from the past three years
today = current_date()
df_transactions = df_transactions.filter((year("transaction_utc_timestamp") >= year(date_sub(today, 1095))) & (df_transactions.transaction_type=='purchase'))

# Group the DataFrame by product, year, and month and calculate total sales
sales = df_transactions.groupBy("product_id", "year", "month").agg(round(sum("price"), 2).alias("total_sales"))

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

# Filter the DataFrame to include only transactions from the past three years
# Get the last day of the previous month
now = datetime.datetime.now()
last_month = now.month - 1 if now.month > 1 else 12
last_month_year = now.year if now.month > 1 else now.year - 1
last_day_of_last_month = datetime.date(year = last_month_year, month = last_month,day =  1) + pd.offsets.MonthEnd(1) - datetime.timedelta(days=1)

# Get the last day of the current month
last_day_of_this_month = datetime.date(now.year, now.month, 1) + pd.offsets.MonthEnd(1) - datetime.timedelta(days=1)

# Filter the DataFrame to only include rows where the date column is from a completed month
if now.day == last_day_of_this_month.day:
    df_filtered_transactions = df_transactions[df_transactions['date'] <= last_day_of_this_month]
else:
    df_filtered_transactions = df_transactions[df_transactions['date'] <= last_day_of_last_month]

# Print the filtered DataFrame
print(df_filtered_transactions)

# COMMAND ----------

from pyspark.sql.functions import year, month

# Assume you have a PySpark DataFrame named "df" with a date column named "date_column"

# Extract the year and month from the date column
df_filtered_transactions = df_filtered_transactions.withColumn('year', year(df_filtered_transactions['date']))
df_filtered_transactions = df_filtered_transactions.withColumn('month', month(df_filtered_transactions['date']))

# Print the DataFrame with the new columns
df_filtered_transactions.show()


# COMMAND ----------

print(type(df_filtered_transactions))

# COMMAND ----------

# Group the DataFrame by product, year, and month and calculate total sales
sales = df_filtered_transactions.groupBy("product_id", "year", "month").agg(round(sum("price"), 2).alias("total_sales"))

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
sales.select("product_name", "year", "aliased_month", "total_sales","transaction_type").display()

# COMMAND ----------

# Step 1: Create a signed_price column
df_filtered_transactions = df_filtered_transactions.withColumn(
    "signed_price",
    when(col("transaction_type") == "return", -col("price")).otherwise(col("price")),
)

# Step 2: Aggregate by email, product_id, and transaction_date
df_grouped_transactions = df_filtered_transactions.groupBy(
    "email", "product_id", "date","year","month","transaction_type"
).agg(sum("signed_price").alias("daily_signed_price"))

# Step 3: Use lag function to find previous purchase date for each returned product
window_spec = Window.partitionBy("email", "product_id").orderBy("date")
df_grouped_transactions = df_grouped_transactions.withColumn(
    "previous_purchase_signed_price",
    lag("daily_signed_price").over(window_spec),
)

# Step 4: Group by product_id, year, and month to calculate total sales
sales = (
    df_grouped_transactions.withColumn(
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

sales.select("product_name", "year", "aliased_month", "total_sales","transaction_type").display()

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

# Print the resulting DataFrame
sorted_sales.show()

# COMMAND ----------


