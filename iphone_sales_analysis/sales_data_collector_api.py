Python Code:

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Data Collector").enableHiveSupport().getOrCreate()


def sales_data_collector_api(spark, text_file_path):
    # Load the sales data from the given text file with '|' delimiter
    sales_df = spark.read.option("header", True).option("delimiter", "|").csv(text_file_path)
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    # Write the data to a Hive table with partitioning on 'sale_date'
    sales_df.write.mode("overwrite").partitionBy("sale_date").format("parquet").saveAsTable("iphone_sales_analysis1.sales_hive_table")

    return "iphone_sales_analysis1.sales_hive_table"

if __name__ == '__main__':
    filepath = 'file:///home/takeo/data/iphone_sales_analysis_project/sales_data'
    sales_data_collector_api(spark, filepath)
