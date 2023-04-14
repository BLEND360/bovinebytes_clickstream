import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
from blend360_all_star_clickstream_api.datafetch import DataFetch

# Constants
DEST_BUCKET = 'allstar-training-bovinebytes'
TABLES_TYPE1 = ['products', 'users']
TABLES_TYPE2 = ['transactions', 'clickstream']

def init_spark():
    spark = SparkSession.builder.appName("myApp").getOrCreate()
    return spark

def load_tables(spark):
    df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
    df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")
    return df_transactions, df_products

def get_dates(df_transactions):
    start_date = df_transactions.agg({"date": "max"}).collect()[0][0]
    one_day = datetime.timedelta(days=1)
    start_date = start_date + one_day
    start_date = start_date.date()

    end_date = datetime.datetime.now().date()
    end_date = end_date - one_day

    year = str(start_date.year)
    
    return start_date, end_date, year

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

def main(start_date, end_date):
    spark = init_spark()
    df_transactions, df_products = load_tables(spark)
    start_date, end_date, year = get_dates(df_transactions)
    data_fetch = DataFetch(secret_scope='my-scope', key_name='api-key')

    for table in TABLES_TYPE2:
        print('Working on' + table)
        dest_directory = f'bronze/{table}/{year}'
        job_id = fetch_data(data_fetch, start_date, end_date, table, DEST_BUCKET, dest_directory)
        wait_for_job_completion(data_fetch, job_id)

    for table in TABLES_TYPE1:
        print('Working on' + table)
        dest_directory = f'bronze/{table}'
        job_id = fetch_data(data_fetch, start_date, end_date, table, DEST_BUCKET, dest_directory)
        wait_for_job_completion(data_fetch, job_id)

    df_transactions.show()
    df_products.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date", type=str, default="2020-01-01")
    parser.add_argument("--end_date", type=str, default=datetime.datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()

    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d').date()

    main(start_date, end_date)
