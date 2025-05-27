import sys

from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J
from lib.utils import *

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: DataSinkDemo <filename>")
        sys.exit(-1)

    logger.info("Starting DataSinkDemo")

    flightParquetDF = load_flight_parquet_df(spark, sys.argv[1])
    flightParquetDF.show(10, False)
    logger.info("parquet schema:" + flightParquetDF.schema.simpleString())

    flightParquetDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/") \
        .save()

    partitionedDF = flightParquetDF.repartition(5)

    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/partitioned/") \
        .save()

    logger.info("Number of partitions:" + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    flightParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("carrier", "origin") \
        .option("maxRecordsPerFile", 10000) \
        .save()

    logger.info("DataSinkDemo Stopped!")
