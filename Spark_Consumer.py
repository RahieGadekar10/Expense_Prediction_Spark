import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
import pandas as pd
import SparkTransfomer
import argparse
from utility import read_params
import mongo
import AppLogger

class Consumer:
    def __init__(self,config):
        self.config = config
        self.spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
        self.spark_transformer = SparkTransfomer.SparkTransformer()
        self.kafka_bootstrap_server = self.config['kafka']['kafka_bootstrap_server']
        self.kafka_topic_name = self.config['kafka']['kafka_topic_name']
        self.mongodb = mongo.Mongo(self.config)
        self.schema = self.config['kafka']['schema']
        self.streaming_database = self.config['mongodb']['streaming_database']
        self.streaming_collection = self.config['mongodb']['streaming_collection']
        self.logger_database = self.config['log_database']['log_training_database']
        self.logger_collection = self.config['log_database']['log_training_collection']
        self.logger = AppLogger.AppLogger(self.config, self.logger_database , self.logger_collection)
        self.models = []
        self.prediction_columns = self.config['mongodb']['prediction_columns']

    def consume(self):
        dataframe = self.spark.readStream.format("kafka").option("kafka.bootstrap.servers",self.kafka_bootstrap_server).option("subscribe",self.kafka_topic_name).option("startingOffsets","latest").load()
        dataframe1 = dataframe.selectExpr("CAST(value as STRING)","timestamp")
        dataframe2 = dataframe1.select(from_csv(functions.col("value"), self.schema).alias("records"),"timestamp")
        dataframe3 = dataframe2.select("records.*","timestamp")
        transformed_df = dataframe3
        for model in self.models :
            transformed_df = model.transform(transformed_df)
        query = transformed_df.writeStream.format("console").trigger(processingTime="5 seconds").foreachBatch(self.mongodb.process_each_record).start()
        query.awaitTermination()

    def add_machine_learning_model(self, ml_models:list) :
        self.models.extend(ml_models)

def main(config_path : str ) :
    config1 = read_params(config_path=config_path)
    kafkaconsumer = Consumer(config=config1)
    kafkaconsumer.consume()

if __name__ == "__main__" :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default= os.path.join("config","params.yaml"))
    parsed_args = args.parse_args()
    main(config_path=parsed_args.config)