Python code:

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Product Data Collector") \
    .enableHiveSupport() \
    .getOrCreate()

def product_data_collector_api(spark, text_file_path, parquet_file_path):
    # Step 1: Load the CSV data from the text file with '|' delimiter
    product_df = spark.read.option("header", True).option("delimiter", "|").csv(text_file_path)

    # Step 2: Write the DataFrame to Parquet format
    product_df.write.mode("overwrite").parquet(parquet_file_path)

    # Step 3: Read the Parquet data back into a DataFrame
    parquet_df = spark.read.parquet(parquet_file_path)

    # Step 4: Write the data to a Hive table from the Parquet DataFrame
    parquet_df.write.mode("overwrite").saveAsTable("iphone_sales_analysis1.product_hive_table")

    return "iphone_sales_analysis1.product_hive_table"

if __name__ == '__main__':
    # Define file paths
    csv_filepath = 'file:///home/takeo/data/iphone_sales_analysis_project/product_data/product_data_csv'
    parquet_filepath = 'file:///home/takeo/data/iphone_sales_analysis_project/product_data/product_data_parquet'

    # Call the product data collector function
    product_data_collector_api(spark, csv_filepath, parquet_filepath)
