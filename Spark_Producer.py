from kafka import KafkaProducer
import os
import SparkTransfomer
import time
import AppLogger
from pyspark.sql import SparkSession
from utility import read_params
import argparse

class Kafka_Producer :
    def __init__(self, config, is_log_enabled =True):
        self.config = config
        self.kafka_bootstrap_server = self.config['kafka']['kafka_bootstrap_server']
        self.kafka_topic_name = self.config['kafka']['kafka_topic_name']
        self.spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
        self.logging_database = self.config['log_database']['log_training_database']
        self.logger_collection =  self.config['log_database']['log_training_collection']
        self.logger = AppLogger.AppLogger(self.config , self.logging_database , self.logger_collection)
        self.training_file_path = self.config['training_data']['training_file_path']
        self.prediction_file_path = self.config['prediction_data']['prediction_file']
        self.is_log_enabled = is_log_enabled

    def Produce(self):
        kafka_producer = KafkaProducer(bootstrap_servers = self.kafka_bootstrap_server,
                                       value_serializer = lambda x : x.encode("utf-8"))
        dirs = os.listdir(self.prediction_file_path)
        nrow = 0
        for file in dirs :
            if not file.endswith(".csv") :
                continue
            else :
                spark = SparkSession.builder.master("local").appName("IncomePrediction").config("spark.jars.package","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"). config("spark.ui.port", "4041").getOrCreate()
                df = spark.read.csv(os.path.join(self.prediction_file_path , file) , header=True , inferSchema= True)
                for row in df.rdd.toLocalIterator() :
                    row_send = ','.join(map(str , list(row)))
                    print(row_send)
                    kafka_producer.send(self.kafka_topic_name,row_send)
                    nrow+=1
                    time.sleep(1)
        self.logger.log(f"Total Rows sent : {nrow}")
        return nrow

def main(config_path : str) :
    config1 = read_params(config_path=config_path)
    producer = Kafka_Producer(config=config1)
    producer.Produce()

if __name__ == "__main__" :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default= os.path.join("config" , "params.yaml"))
    parsed_args = args.parse_args()
    main(config_path=parsed_args.config)


