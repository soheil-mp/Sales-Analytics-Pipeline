from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

def read_sales_data(spark, path):
    """Read sales data from CSV file"""
    schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("sale_date", DateType(), True),
        StructField("sale_amount", DoubleType(), True),
        StructField("customer_id", StringType(), True)
    ])

    return spark.read.csv(path, header=True, schema=schema)