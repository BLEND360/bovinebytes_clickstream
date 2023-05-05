import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, countDistinct, round, when
from pyspark.sql.types import *

from blend360_all_star_clickstream_api.datafetch import DataFetch

def fetch_data(data_fetch, start_date, end_date, table_name, dest_bucket, dest_directory):
    '''
    Function to ingest data from the API using the python package

    Params:
    data_fetch: contains the api key
    start_date: start date of when the data should be ingested from
    end_date: end data till when the data should be ingested to
    table_name: name of the table - [transactions, products, users, clickstream]
    dest_bucket: s3 bucket name
    dest_directory: directory to store in s3 bucket
    '''
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
    '''
    Function to check the status of the ingestion job

    Params:
    data_fetch: contains the api key
    job_id: the job id returned from the fetch api
    '''
    response = data_fetch.checkStatus(job_id=str(job_id))
    
    if response.status_code == 200:
        data = response.json()
        return data['execution_status']
    else:
        print("Error:", response.text)

def wait_for_job_completion(data_fetch, job_id):
    '''
    Function to check the job status until its complete
    '''
    while True:
        job_status = check_status(data_fetch, job_id)
        print(job_status)
        if job_status == 'COMPLETE':
            break
        else:
            time.sleep(10)