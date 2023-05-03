from pyspark.sql.functions import from_unixtime, from_utc_timestamp, current_timestamp, date_trunc
from pyspark.sql.functions import max
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("myApp").getOrCreate()

# Load four tables from Amazon S3 into Spark DataFrames
# Load the 'users' table from the 'bronze' directory
df_users = spark.read.format("delta").load("s3://allstar-training-bovinebytes/bronze/users/")

# Load the 'products' table from the 'bronze' directory
df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/bronze/products/")

# Load the 'clickstream' table from the 'bronze' directory and enable recursive file lookup
df_clickstream = spark.read.option("recursiveFileLookup", "true").parquet("s3://allstar-training-bovinebytes/bronze/clickstream/")

# Load the 'transactions' table from the 'bronze' directory and enable recursive file lookup
df_transactions = spark.read.option("recursiveFileLookup", "true").parquet("s3://allstar-training-bovinebytes/bronze/transactions/")


def load_users_to_silver():
    # Add new columns to the 'df_users' DataFrame to convert the Unix timestamp and add the ETL loaded timestamp
    silver_df_users_tmp = df_users.withColumn("first_timestamp", from_unixtime("first_time_time_stamp"))
    silver_df_users_tmp = silver_df_users_tmp.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

    # silver_df_users_tmp = silver_df_users_tmp.withColumn("date", date_trunc("day", "first_timestamp"))

    # Select only the required columns for the new silver table
    silver_df_users_tmp = silver_df_users_tmp.select("account_id", "email", "first_timestamp", "etl_loaded_at")

    silver_df_users_tmp.write.format("delta").mode("overwrite").save("s3://allstar-training-bovinebytes/silver/users/")

def load_products_to_silver():
    # Add a new column to the 'df_products' DataFrame to add the ETL loaded timestamp
    silver_df_products_tmp = df_products.withColumn("etl_loaded_at", from_utc_timestamp(current_timestamp(), "UTC"))

    # Write the new DataFrame to a parquet file in Amazon S3
    silver_df_products_tmp.write.format("delta").mode("overwrite").save("s3://allstar-training-bovinebytes/silver/products/")

    # Read parquet files from Amazon S3 for silver products table
    silver_df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")

# Create a new DataFrame that combines information from 'df_transactions' and 'df_products'

def load_transactions_to_silver():
    silver_df_transactions_get_max = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
    max_date = silver_df_transactions_get_max.agg(max("date")).collect()[0][0]
    print(max_date)

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


# CREATING CLICKSTREAM SILVER TABLE

def load_clickstream_to_silver():
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

if __name__ == '__main__':
    load_users_to_silver()
    load_products_to_silver()
    load_transactions_to_silver()
    load_clickstream_to_silver()
