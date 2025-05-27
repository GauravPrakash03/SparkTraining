import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSparkSQL <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSparkSQL")
    #your processing code
    #conf_out = spark.sparkContext.getConf()
    #logger.info(conf_out.toDebugString())
    survey_df_raw = load_survey_df(spark, sys.argv[1])
    survey_df = survey_df_raw \
        .withColumnRenamed("transaction id", "transactionId") \
        .withColumnRenamed("product id", "productId") \
        .withColumnRenamed("quantity sold", "quantitySold") \
        .withColumnRenamed("transaction timestamp", "transactionTS") \
        .withColumnRenamed("unit price", "unitPrice") \
        .withColumnRenamed("customer id", "customerId") \
        .withColumnRenamed("transaction country", "transactionCountry")

    survey_df.createOrReplaceTempView("survey_tbl")
    count_df = spark.sql("SELECT productId, COUNT(1) FROM survey_tbl where quantitySold > 5 GROUP BY productId")
    count_df.show()