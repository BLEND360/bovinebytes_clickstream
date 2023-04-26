import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, countDistinct, round, when
from pyspark.sql.types import *
import argparse
import json
import requests
from blend360_all_star_clickstream_api.datafetch import DataFetch
from data_fetching import *

def fetch_data(data_fetch, start_date, end_date, table_name, dest_bucket, dest_directory):
    response = data_fetch.fetchData(
        start_date=start_date,
        end_date=end_date,
        destination_bucket=dest_bucket,
        destination_directory=dest_directory,
        table_name=table_name
    )
    
    if response.status_code == 200:
        data = response.json()
        job_id = data['job_id']
        print(f'Your JobID is {job_id}')
        return job_id
    else:
        print("Error:", response.text)

def check_status(data_fetch, job_id):
    response = data_fetch.checkStatus(job_id=str(job_id))
    
    if response.status_code == 200:
        data = response.json()
        return data['execution_status']
    else:
        print("Error:", response.text)

def wait_for_job_completion(data_fetch, job_id):
    while True:
        job_status = check_status(data_fetch, job_id)
        print(job_status)
        if job_status == 'COMPLETE':
            break
        else:
            time.sleep(10)