def save_to_hive(spark, df, database, table):
    """Save DataFrame to Hive table"""
    df.write.mode("overwrite").saveAsTable(f"{database}.{table}")

def save_to_hdfs(df, path):
    """Save DataFrame to HDFS in Parquet format"""
    df.write.mode("overwrite").parquet(path)