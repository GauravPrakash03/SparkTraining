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
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
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

    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_productId(partitioned_survey_df)
    logger.info(count_df.collect())

    #input("Press Enter to continue...")

    logger.info("Finished HelloSpark")
    spark.stop()
