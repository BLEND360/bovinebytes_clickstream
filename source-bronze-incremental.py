import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from blend360_all_star_clickstream_api.datafetch import DataFetch
from data_fetching import fetch_data, wait_for_job_completion
from quality_assurance import *


# Constants
DEST_BUCKET = "allstar-training-cowcode"
TABLES_TYPE1 = ['products', 'users']
TABLES_TYPE2 = ['transactions', 'clickstream']
LAYERS = ['bronze', 'silver', 'gold']

PREFIX = 'bovinebytes_copy/'

def init_spark():
    '''
    Initializes a SparkSession object with the name "myApp".

    Returns:
    ----------
    SparkSession object.
    '''
    spark = SparkSession.builder.appName("myApp").getOrCreate()
    return spark

def load_tables(spark):
    '''
    Loads delta tables from S3 into dataframes using a SparkSession object.

    Parameters:
    -----------
    spark : SparkSession object.

    Returns:
    ----------
    Tuple of 3 dataframes: (df_clickstream, df_transactions, df_products).
    '''
    df_transactions = spark.read.format("delta").load(f"s3://{DEST_BUCKET}/{PREFIX}silver/transactions/")
    df_clickstream = spark.read.format("delta").load(f"s3://{DEST_BUCKET}/{PREFIX}silver/clickstream/")
    df_products = spark.read.format("delta").load(f"s3://{DEST_BUCKET}/{PREFIX}silver/products/")
    return df_clickstream, df_transactions, df_products

def get_dates(df_transactions):
    '''
    Computes start and end dates for data fetching and quality assurance based on the latest transaction date.

    Parameters:
    -----------
    df_transactions : Dataframe containing the transactions table.

    Returns:
    ----------
    Tuple of 3 datetime.date objects: (start_date, end_date, year).
    '''
    start_date = df_transactions.agg({"date": "max"}).collect()[0][0]
    one_day = datetime.timedelta(days=1)
    start_date = start_date + one_day
    start_date = start_date.date()

    end_date = datetime.datetime.now().date()
    end_date = end_date - one_day

    year = str(start_date.year)

    return start_date, end_date, year

def data_QA_history(table='clickstream', layer='bronze'):
    '''
    Calls quality assurance function for a specific table and layer.

    Parameters:
    -----------
    table : str, optional, default is 'clickstream'
        The name of the table to be quality assured.
    layer : str, optional, default is 'bronze'
        The name of the layer where the table is stored.
    '''
    source_directory = f'{PREFIX}{layer}/{table}/'
    spark = init_spark()
    df_clickstream, df_transactions, df_products = load_tables(spark)
    if table == 'transactions':
        start_date = df_transactions.agg({"date": "min"}).collect()[0][0]
        end_date = df_transactions.agg({"date": "max"}).collect()[0][0]
    elif table == 'clickstream':
        start_date = df_clickstream.agg({"date": "min"}).collect()[0][0]
        end_date = df_clickstream.agg({"date": "max"}).collect()[0][0]

    data_fetch = DataFetch(secret_scope="de-all-star-cowcode", key_name="api-key")
    quality_assurance_call(data_fetch, spark, start_date, end_date, 'bronze', table, source_directory, threshold=0.99, test=True)

def transactions_QA(layer, table, year):
    '''
    Calls quality assurance function for the transactions table.

    Parameters:
    -----------
    layer : str
        The name of the layer where the table is stored.
    table : str
        The name of the table to be quality assured.
    year : str
        The year of the data to be quality assured.
    '''
    source_directory = f'{PREFIX}{layer}/{table}/{year}'
    spark = init_spark()
    transactions_quality_assurance(spark, source_directory, year, table, layer)


def main(start_date, end_date):
    '''
    Main function that orchestrates data fetching, quality assurance and loading into bronze layer.

    Parameters:
    -----------
    start_date : datetime.date object
        Start date for data fetching.
    end_date : datetime.date object
        End date for data fetching.
    '''
    spark = init_spark()
    df_clickstream, df_transactions, df_products = load_tables(spark)
    # scope has been set to fetch data using cowcode api key
    data_fetch = DataFetch(secret_scope='de-all-star-cowcode', key_name='api-key')

    for table in TABLES_TYPE2:
        print('Working on' + table)
        #changing the structure of the folder to be based on each fetch
        if table == 'clickstream':
            start_date, end_date, year = get_dates(df_clickstream)
        elif table == 'transactions':
            start_date, end_date, year = get_dates(df_transactions)
            continue

        # if data is up to date, skip the job
        if (datetime.datetime.now().date() - start_date).days <= 0:
            print(f"{table} data is up to date")
            continue

        dest_directory = f'{PREFIX}bronze/{table}/{year}'
        job_id = fetch_data(data_fetch, start_date, end_date, table, DEST_BUCKET, dest_directory)
        wait_for_job_completion(data_fetch, job_id)
        # for clickstream data, conduct QA
        if table == 'clickstream':
            quality_assurance_call(data_fetch, spark, start_date, end_date, 'bronze', table, f'{PREFIX}bronze/{table}', 0.99)

    for table in TABLES_TYPE1:
        print('Working on' + table)
        dest_directory = f'{PREFIX}bronze/{table}'
        job_id = fetch_data(data_fetch, start_date, end_date, table, DEST_BUCKET, dest_directory)
        wait_for_job_completion(data_fetch, job_id)

    df_transactions.show()
    df_products.show()

if __name__ == "__main__":
    start_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    end_date = datetime.datetime.now().date() - datetime.timedelta(days=1)

    main(start_date, end_date)
    data_QA_history()
    transactions_QA(layer=LAYERS[0], table=TABLES_TYPE2[0], year='2022')

   
