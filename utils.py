import configparser

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key,val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key,val)
        return spark_conf

def load_survey_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)

def load_flight_parquet_df(spark, data_file):
    return spark.read \
        .format("parquet") \
        .load(data_file)

def count_by_productId(survey_df):
    return survey_df \
        .where("quantitySold > 5") \
        .select("transactionId", "productId", "quantitySold") \
        .groupBy("productId") \
        .count()