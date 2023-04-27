import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, countDistinct, round, when
from pyspark.sql.types import *
import json
import requests
from data_fetching import fetch_data, check_status, wait_for_job_completion

'''some constants have been changed for testing'''
# Constants
DEST_BUCKET = "allstar-training-cowcode"
TABLES_TYPE1 = ['products', 'users']
TABLES_TYPE2 = ['transactions', 'clickstream']
#prefix is used when a copy is created for testing reasons
PREFIX = 'bovinebytes_copy/'

'''
    visitors are uniquely identified by visid_low and visid_high (combined)
    hits are uniquely identified by hitid_low and hitid_high (combined)
'''
def count_visid_hitid(df):
    df_append = df.withColumn('vistor_id', concat('visid_high', 'visid_low'))
    df_append = df_append.withColumn('hit_id', concat('hitid_high', 'hitid_low'))

    #count distinct visitor_id and hit_id in dataset, and store them into two dataframes
    visitors = df_append.groupBy('utc_date').agg(countDistinct('vistor_id').alias('num_of_visid'))
    hits = df_append.groupBy('utc_date').agg(countDistinct('hit_id').alias('num_of_hitid'))

    return visitors, hits

def check_missing_data(spark,start_date, end_date, clickstream_data,threshold, test = False):
    # create schema for log table
    log_table_schema = StructType([
    StructField("utc_date", DateType(), True),
    StructField("hits_num", IntegerType(), True),
    StructField("visitors_num", IntegerType(), True)])
    
    #StructField("reach_threshold", BooleanType(), True)
    COMP = True
    logs = []
    #get count of hits and visitors
    clickstream_visitors, clickstream_hits = count_visid_hitid(clickstream_data)

    #iterate through all dates within the timeframe
    curr_date = start_date
    delta = (end_date - start_date).days + 1
    if test:
        # if timeframe is less than or equal to 30 days, retain it, elsewise test the first 31 days
        if delta > 30:
            end_date = curr_date + datetime.timedelta(days = 30)

    print(f"Conducting QA check on data from {curr_date} to {end_date}")

    while curr_date <= end_date:
        check_data = {'date':curr_date.strftime("%m-%d-%Y")}
        hits_response = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/metrics/hits', data=json.dumps(check_data))
        # hits_num is the number of hits in the metrics, which is the actual number of hits, type == int
        hits_num = hits_response.json()
        
        visitors_response = requests.get('https://en44bq5e33.execute-api.us-east-1.amazonaws.com/dev/metrics/visitors', data=json.dumps(check_data))
        # visitors_num is the number of visitors in the metrics, which is the actual number of visitors, type == int
        visitors_num = visitors_response.json()

        logs.append((curr_date,hits_num,visitors_num))
        curr_date += datetime.timedelta(days=1)
    
    log_table = spark.createDataFrame(logs, schema = log_table_schema)
    # join log table and counts of hits/visitors
    log_table = log_table.join(clickstream_hits,on="utc_date", how="left")
    log_table = log_table.join(clickstream_visitors,on="utc_date", how="left")
    # fill all those does not exist to 0
    log_table = log_table.fillna(0)
    #calculate completeness
    log_table = log_table.withColumn("hit_completeness", round(col("num_of_hitid") / col("hits_num"),4))
    log_table = log_table.withColumn("visitors_completeness", round(col("num_of_visid") / col("visitors_num"),4))
    log_table = log_table.withColumn("reach_threshold", when((log_table["hit_completeness"] >= 0.99) & (log_table["visitors_completeness"] >= 0.99), True).otherwise(False))

    if log_table.filter(col("reach_threshold") == False).count() > 1:
        COMP = False
    
    return COMP, log_table

def quality_assurance_process(data_fetch,spark,start_date, end_date, year, layer, table, source_directory, threshold, test = False):
    #read stored data of the given table for the given timeframe
    s3_source_directory = f's3://{DEST_BUCKET}/{source_directory}/{year}/*'
    clickstream_data = spark.read.parquet(s3_source_directory)
    clickstream_data = clickstream_data.filter(clickstream_data.utc_date >= start_date).filter(clickstream_data.utc_date <= end_date)

    # Check if the original fetched data reach the threshold, keep a log for the rate
    COMP, initial_log_table = check_missing_data(spark,start_date,end_date,clickstream_data,threshold,test)

    # fetching new data, only allow at most 1 more fetches
    num_of_run = 1
    merged_data = clickstream_data
    while not COMP and num_of_run < 3:
        # get a unique timestamp for the current time
        ct = datetime.datetime.now()
        ts = ct.strftime('%Y%m%d_%H%M%S')
        # set up QA directory to store QA data based on table, year and timestamp
        QA_directory =  f'{PREFIX}{layer}/QA/{table}/{year}/attempt{ts}'
        job_id = fetch_data(data_fetch,start_date, end_date, table, DEST_BUCKET, QA_directory)
        wait_for_job_completion(data_fetch, job_id)
        # read latest fetched data for the given timeframe
        QA_data = spark.read.parquet(f's3://{DEST_BUCKET}/{QA_directory}/*')
        # merge the original fetched data and the QA data, only keep unique rows
        merged_data = QA_data.union(merged_data).distinct()
        # check again to see if we reached desired completeness rate
        COMP, QA_log_table = check_missing_data(spark,start_date,end_date,merged_data,threshold, test)
        num_of_run += 1
    

    # get the difference between merged_data and original fetched data, and store the rows that exist in merged_data but not in bronze layer into bronze layer
    difference = merged_data.subtract(clickstream_data)
    difference.write.mode("append").parquet(f's3://{DEST_BUCKET}/{source_directory}/{year}')

    if num_of_run > 1:
        ct = datetime.datetime.now()
        ts = ct.strftime('%Y%m%d_%H%M%S')
        #set directory to store log table
        log_directory = f's3://{DEST_BUCKET}/{PREFIX}{layer}/QA/{table}/logs/{ts}/'
        #merge initial log table and QA log table together
        log_table = QA_log_table.join(initial_log_table, "utc_date").select(QA_log_table.utc_date.alias('utc_date'), 
                                                                    initial_log_table.num_of_hitid.alias('initial_number_of_hits'), 
                                                                    QA_log_table.num_of_hitid.alias('QA_number_of_hits'), 
                                                                    QA_log_table.hits_num.alias('desired_number_of_hits'), 
                                                                    initial_log_table.hit_completeness.alias('initial_hit_completeness_rate'), 
                                                                    QA_log_table.hit_completeness.alias('QA_hit_completeness_rate'), 
                                                                    initial_log_table.num_of_visid.alias('initial_number_of_visitors'), 
                                                                    QA_log_table.num_of_visid.alias('QA_number_of_visitors'),
                                                                    QA_log_table.visitors_num.alias('desired_number_visitors'), 
                                                                    initial_log_table.visitors_completeness.alias('initial_visitors_completeness_rate'), 
                                                                    QA_log_table.visitors_completeness.alias('QA_visitors_completeness_rate'), 
                                                                    QA_log_table.reach_threshold.alias('data_quality_reached_expectation'))
        log_table = log_table.fillna(0)
        #store log table into s3 bucket
        log_table.write.format("delta").mode("overwrite").save(log_directory)
        print(f'log file for timeframe {start_date} to {end_date} was saved to {log_directory}.')
        log_table.display()
    else:
        print('Initial data meet expectation, no reloading performed.')

    if COMP:
        print(f'All hit completeness rate and visitors completeness rate reach requirement for timeframe {start_date} to {end_date}.')
    else:
        print(f'Requirement not reached for timeframe {start_date} to {end_date}, please refer to log file.')


def quality_assurance_call(data_fetch,spark,start_date, end_date, layer, table, source_directory,threshold = 0.99, test = False):
    print(source_directory)
    
    # if the timeframe is larger than one year, perform QA on yearly data
    for year in range(start_date.year, end_date.year + 1):
        # Set start and end months for the current year
        start_month = 1 if year > start_date.year else start_date.month
        end_month = 12 if year < end_date.year else end_date.month
        ''' set start day for the current year
             1) if the current year is the start year, the first day would be start date, and therefore the start_day would be
             the day of start date
             2) if the current year is not the start year, we would fetch from January 1st, and therefore start day would be 1
        '''
        start_day = 1 if year > start_date.year else start_date.day
        '''
            1) if the current year is the end year, the last day would be end date, and therefore the end_day would be the day
            of the end day
            2) if the current year is not the end year, we would fetch data until December 31st.
        '''
        end_day = 31 if year < end_date.year else end_date.day

        first_day = datetime.date(year, start_month, start_day)
        last_day = datetime.date(year, end_month, end_day)

        quality_assurance_process(data_fetch,spark,first_day, last_day, year, layer, table, source_directory,threshold, test)
    


