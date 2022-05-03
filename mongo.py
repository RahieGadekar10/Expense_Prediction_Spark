import pymongo
from pymongo import MongoClient
import json
import argparse
import os
from utility import read_params
import pandas as pd

class Mongo :
    def __init__(self, config):
        self.config = config
        self.url = self.config['mongodb']['url']
        self.training_database_name = self.config['log_database']['log_training_database']
        self.training_collection_name = self.config['log_database']['log_training_collection']
        self.streaming_database = self.config['mongodb']['streaming_database']
        self.streaming_collection = self.config['mongodb']['streaming_collection']
        self.client = MongoClient(self.url)
        self.prediction_columns = self.config['mongodb']['prediction_columns']


    def get_client_object(self):
        return self.client

    def get_database_object(self, client):
        database =  client[self.training_database_name]
        return database

    def get_streaming_database_object(self, client):
        database =  client[self.streaming_database]
        return database

    def get_collection_object(self,database):
        collection_object = database[self.training_collection_name]
        return collection_object

    def get_streaming_collection_object(self,database):
        collection_object = database[self.streaming_collection]
        return collection_object

    def insert_single_record(self , record, collection ) :
        collection.insert_one(record)
        return 1

    def insert_many_records(self, df, collection):
        df.reset_index(drop=True, inplace=True)
        records = list(json.loads(df.T.to_json()).values())
        collection.insert_many(records)
        return len(records)

    def process_each_record(self, df, epoch_id):
        df = df.select(self.prediction_columns)
        df = df.toPandas()
        if df.shape[0] > 0:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            client = self.get_client_object()
            database = self.get_streaming_database_object(client)
            collection_object = self.get_streaming_collection_object(database)
            self.insert_many_records(df=df, collection=collection_object)
            df.to_csv("newdata.csv")

def main(config_path : str ) :
    config1 = read_params(config_path=config_path)
    mongo = Mongo(config1)
    client = mongo.get_client_object()
    database = mongo.get_database_object(client=client)
    collection_obj = mongo.get_collection_object(database=database)
    record = {'hi1':'hello1'}
    collection_obj.insert_one(record)


if "__main__"==__name__ :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default= os.path.join("config","params.yaml"))
    parsed_args = args.parse_args()
    main(config_path=parsed_args.config)

