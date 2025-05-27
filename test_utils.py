from unittest import TestCase
from pyspark.sql import *
from lib.utils import *

class UtilsTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
        .master("local[3]") \
        .appName("HelloSparkTest") \
        .getOrCreate()

    def test_datafile_loading(self):
        sample_df = load_survey_df(self.spark, data_file="data/sales_data.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 541909, "Record Count should be 541909")

    def test_productId_count(self):
        sample_df_raw = load_survey_df(self.spark, data_file="data/sales_data.csv")
        sample_df = sample_df_raw \
            .withColumnRenamed("transaction id", "transactionId") \
            .withColumnRenamed("product id", "productId") \
            .withColumnRenamed("quantity sold", "quantitySold") \
            .withColumnRenamed("transaction timestamp", "transactionTS") \
            .withColumnRenamed("unit price", "unitPrice") \
            .withColumnRenamed("customer id", "customerId") \
            .withColumnRenamed("transaction country", "transactionCountry")
        count_list = count_by_productId(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["productId"]] = row["count"]
        self.assertEqual(count_dict["10002"], 33, "Count for productID 10002 should be 33")
        self.assertEqual(count_dict["10080"], 18, "Count for productID 10080 should be 18")
        self.assertEqual(count_dict["10120"], 10, "Count for productID 10120 should be 10")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()