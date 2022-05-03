from pyspark.sql import SparkSession

class SparkTransformer :
    def __init__(self):
        pass
    def SparkSessionObject(self) :
        spark = SparkSession.builder.master("local").appName("IncomePrediction").config("spark.jars.package","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1").\
            config("spark.ui.port","4041").getOrCreate()
        return spark


