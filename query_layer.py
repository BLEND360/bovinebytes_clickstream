import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, round, when, lag, col, max

# Initialize Spark session
spark = SparkSession.builder.appName("myApp").getOrCreate()

# Read gold layer transactions data
gold_df_transactions = spark.read.format("delta").load("s3://allstar-training-bovinebytes/gold/transactions/")

# Get the max date from the dataset and the last day of the previous and current month
now = gold_df_transactions.agg(max('date')).collect()[0][0]
last_month = now.month - 1 if now.month > 1 else 12
last_month_year = now.year if now.month > 1 else now.year - 1
last_day_of_last_month = datetime.date(year = last_month_year, month = last_month,day =  1) + pd.offsets.MonthEnd(1) - datetime.timedelta(days=1)
last_day_of_this_month = datetime.date(now.year, now.month, 1) + pd.offsets.MonthEnd(1) - datetime.timedelta(days=1)

# Filter the DataFrame to include only transactions for product0 from completed months
gold_df_transactions = gold_df_transactions.filter(gold_df_transactions.product_id == "product0")
if now.day == last_day_of_this_month.day:
    df_filtered_transactions = gold_df_transactions[gold_df_transactions['date'] <= last_day_of_this_month]
else:
    df_filtered_transactions = gold_df_transactions[gold_df_transactions['date'] <= last_day_of_last_month]

# Group by product_id, year, and month to calculate total sales
sales = (
    df_filtered_transactions.withColumn(
        "adjusted_signed_price",
        when(
            (col("transaction_type") == "return") & col("previous_purchase_signed_price").isNull(),
            col("daily_signed_price"),
        ).otherwise(col("daily_signed_price") + col("previous_purchase_signed_price")),
    )
    .groupBy("product_id", "year", "month")
    .agg(round(sum("daily_signed_price"), 2).alias("total_sales"))
)

# Calculate the month over month sales growth (amount and percentage)
sorted_sales = sales.orderBy('year', 'month')
sorted_sales = sorted_sales.withColumn('MoM_growth', round(col('total_sales') - lag('total_sales', 1).over(Window.partitionBy('product_id').orderBy('year', 'month')), 2))
sorted_sales = sorted_sales.withColumn('MoM_pct_growth', round((col('MoM_growth')/lag('total_sales', 1).over(Window.partitionBy('product_id').orderBy('year', 'month')))*100, 2))

# Handle special case for the first month (assign 0.0% growth)
sorted_sales = sorted_sales.withColumn('MoM_growth', when(col('MoM_growth').isNull(), col('total_sales')).otherwise(col('MoM_growth')))
sorted_sales = sorted_sales.withColumn('MoM_pct_growth', when(col('MoM_pct_growth').isNull(), 0.0).otherwise(col('MoM_pct_growth')))

# Add product_name and aliased_month columns
sorted_sales = sorted_sales.withColumn("product_name", when(sales.product_id == "product0", "Tumbler"))
sorted_sales = sorted_sales.withColumn("aliased_month",
                                       when(sorted_sales.month == "1", "Jan")
                                       .when(sorted_sales.month == "2", "Feb")
                                       .when(sorted_sales.month == "3", "Mar")
                                       .when(sorted_sales.month == "4", "Apr")
                                       .when(sorted_sales.month == "5", "May")
                                       .when(sorted_sales.month == "6", "Jun")
                                       .when(sorted_sales.month == "7", "Jul")
                                       .when(sorted_sales.month == "8", "Aug")
                                       .when(sorted_sales.month == "9", "Sep")
                                       .when(sorted_sales.month == "10", "Oct")
                                       .when(sorted_sales.month == "11", "Nov")
                                       .when(sorted_sales.month == "12", "Dec"))

# Write the final DataFrame
sorted_sales.select("product_name", "year", "aliased_month", "total_sales", "MoM_growth", "MoM_pct_growth").write.format("delta").save('s3://allstar-training-bovinebytes/sales_report/2023')
