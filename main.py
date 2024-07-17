import logging
import sys
from config import Config
from spark_session import create_spark_session
from data_loader import read_sales_data
from data_processor import clean_data
from data_analyzer import analyze_sales
from data_writer import save_to_hive, save_to_hdfs

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    spark = create_spark_session()

    try:
        logger.info("Reading sales data from HDFS")
        sales_df = read_sales_data(spark, Config.HDFS_INPUT_PATH)

        logger.info("Cleaning and preprocessing data")
        clean_sales_df = clean_data(sales_df)

        logger.info("Performing sales analysis")
        monthly_sales, customer_analysis, category_performance = analyze_sales(clean_sales_df)

        logger.info("Saving results to Hive")
        save_to_hive(spark, clean_sales_df, Config.HIVE_DB, Config.HIVE_TABLE)

        logger.info("Saving analysis results to HDFS")
        save_to_hdfs(monthly_sales, f"{Config.HDFS_OUTPUT_PATH}/monthly_sales")
        save_to_hdfs(customer_analysis, f"{Config.HDFS_OUTPUT_PATH}/customer_analysis")
        save_to_hdfs(category_performance, f"{Config.HDFS_OUTPUT_PATH}/category_performance")

        logger.info("Analysis complete")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()