import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from lib.logger import Log4J

SurveyRecord = namedtuple("SurveyRecord", ["transactionId", "productId",
                                           "quantitySold", "transactionTS",
                                           "unitPrice", "customerId", "transactionCountry"])

if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD") \
    #sc = SparkContext(conf=conf)

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    sc = spark.sparkContext
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloRDD <filename>")
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionRDD = linesRDD.repartition(2)

    colsRDD = partitionRDD.map(lambda line: line.split(','))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(cols[1], cols[2], cols[3]
                                                      , cols[4], cols[5], cols[6], cols[7]))
    filterRDD = selectRDD.filter(lambda r: r.quantitySold > 5)
    kvRDD = filterRDD.map(lambda r: (r.productId, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for col in colsList:
        logger.info(col)

