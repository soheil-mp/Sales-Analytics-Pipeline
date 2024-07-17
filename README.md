# Sales-Analytics-Pipeline

A comprehensive data analytics pipeline built with Apache Spark and Hadoop for processing and analyzing large-scale sales data.

This project demonstrates how to read and write data from and to HDFS, clean and preprocess data using PySpark, conduct advanced analytics with Spark SQL and window functions, integrate with Hive for data warehousing, and maintain a modular code structure for complex ETL processes.

Key features include scalable sales data processing, monthly sales trend analysis, insights into customer purchasing behavior, evaluation of product category performance, configurable data input/output paths, and robust error handling and logging.

The tech stack utilized in this project comprises Apache Spark, the Hadoop Distributed File System (HDFS), Apache Hive, and Python.

Run the following command to start the codes:
```bash
$ spark-submit --master yarn \
               --deploy-mode client \
               --driver-memory 2g \
               --executor-memory 2g \
               --executor-cores 2 \
               main.py
```
