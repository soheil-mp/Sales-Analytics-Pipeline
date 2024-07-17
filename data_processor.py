from pyspark.sql.functions import col, year, month

def clean_data(df):
    """Clean and preprocess the data"""
    return (df.dropDuplicates(["sale_id"])
            .filter(col("sale_amount") > 0)
            .withColumn("year", year(col("sale_date")))
            .withColumn("month", month(col("sale_date"))))