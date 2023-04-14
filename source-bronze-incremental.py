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
