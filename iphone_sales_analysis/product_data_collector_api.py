Python code:

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Product Data Collector").enableHiveSupport().getOrCreate()

def product_data_collector_api(spark, parquet_file_path):
    # Load the product data from the parquet file
    product_df = spark.read.parquet(parquet_file_path)

    # Save the data to a non-partitioned Hive table
    product_df.write.mode("overwrite").saveAsTable("iphone_sales_analysis.product_hive_table")

    return "iphone_sales_analysis.product_hive_table"

if __name__ == '__main__':
    #File paths
    csv_file_path = 'file:///home/takeo/data/iphone_sales_analysis_project/product_data/product_data_csv'
    parquet_file_path = 'file:///home/takeo/data/iphone_sales_analysis_project/product_data/product_data_parquet'

    #Load CSV data into a dataFrame
    df = spark.read.csv(csv_file_path, header = True, inferSchema = True)

    #Write dataFrame to parquet format
    df.write.parquet(parquet_file_path, mode = 'overwrite')

    #Call function product_data_collector_api after the csv data has been converted into parquet data
    product_data_collector_api(spark, parquet_file_path)
