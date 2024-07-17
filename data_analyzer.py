from pyspark.sql.functions import col, avg, count, sum
from pyspark.sql.window import Window

def analyze_sales(df):
    """Perform various analyses on the sales data"""
    # Monthly sales analysis
    monthly_sales = (df.groupBy("year", "month", "product_category")
                     .agg(sum("sale_amount").alias("total_sales"),
                          count("*").alias("num_transactions"))
                     .orderBy("year", "month", "total_sales", ascending=False))

    # Customer analysis
    customer_window = Window.partitionBy("customer_id")
    customer_analysis = (df.withColumn("total_customer_purchases", sum("sale_amount").over(customer_window))
                         .withColumn("customer_purchase_count", count("*").over(customer_window))
                         .select("customer_id", "total_customer_purchases", "customer_purchase_count")
                         .distinct()
                         .orderBy("total_customer_purchases", ascending=False))

    # Product category performance
    category_performance = (df.groupBy("product_category")
                            .agg(sum("sale_amount").alias("category_total_sales"),
                                 avg("sale_amount").alias("category_avg_sale"),
                                 count("*").alias("category_sale_count"))
                            .orderBy("category_total_sales", ascending=False))

    return monthly_sales, customer_analysis, category_performance