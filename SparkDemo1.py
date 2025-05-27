from Tools.scripts.dutree import display
from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()
    data_list = [("Ravi", 28), ("David", 45), ("Abdul", 37)]
    df = spark.createDataFrame(data_list).toDF("name", "age")
    df.show()
    sales_df =  spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("C:/Gaurav_Prakash/Spark_Learning/sales_data.csv")
#    sales_df.count()
    print(sales_df.count())
    sales_df.show(10)
#    sales_df.createGlobalTempView("sales_view")