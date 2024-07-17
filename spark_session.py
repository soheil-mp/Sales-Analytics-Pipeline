from pyspark.sql import SparkSession

def create_spark_session():
    """Create and return a Spark session"""
    return (SparkSession.builder
            .appName("ComprehensiveSalesAnalysis")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "1g")
            .enableHiveSupport()
            .getOrCreate())