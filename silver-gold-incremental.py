from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import ( year, month, sum, when, lag, col)

# Initialize Spark session
spark = SparkSession.builder.appName("myApp").getOrCreate()

# Read data from silver layer
silver_df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/transactions/")
silver_df_products = spark.read.format("delta").load("s3://allstar-training-bovinebytes/silver/products/")

# Extract year and month from the date column
df_mod_transactions = silver_df_transactions.withColumn('year', year(silver_df_transactions['date']))
df_mod_transactions = df_mod_transactions.withColumn('month', month(df_mod_transactions['date']))

# Step 1: Create a signed_price column
"""
Create a new column, "signed_price", which contains the price of the product with a
negative sign if the transaction type is 'return'.
"""
df_mod_transactions = df_mod_transactions.withColumn(
    "signed_price",
    when(col("transaction_type") == "return", -col("price")).otherwise(col("price")),
)

# Step 2: Aggregate transactions by email, product_id, date, year, month, and transaction_type
"""
Aggregate transactions by email, product_id, date, year, month, and transaction_type,
calculating the daily signed price.
"""
df_grouped_transactions = df_mod_transactions.groupBy(
    "email", "product_id", "date", "year", "month", "transaction_type"
).agg(sum("signed_price").alias("daily_signed_price"))

# Step 3: Use lag function to find previous purchase date for each returned product
"""
Use the lag function to find the previous purchase date for each returned product,
creating a new column called "previous_purchase_signed_price".
"""
window_spec = Window.partitionBy("email", "product_id").orderBy("date")

df_grouped_transactions = df_grouped_transactions.withColumn(
    "previous_purchase_signed_price",
    lag("daily_signed_price").over(window_spec),
)

# Write the transformed data to the gold layer
"""
Write the transformed data to the gold layer with the following path:
"s3://allstar-training-bovinebytes/gold/transactions".
"""
df_grouped_transactions.write.format("delta").partitionBy("date").mode("append").save("s3://allstar-training-bovinebytes/gold/transactions")
