from pymongo import MongoClient
from datetime import datetime
import config

class AppLogger :
    def __init__(self ,config, logger_database , logger_collection  , executed_by="Rahie" ,  is_logger_enable=True):
        self.logger_database = logger_database
        self.logger_collection = logger_collection
        self.is_logger_enable = is_logger_enable
        self.url = config['mongodb']['url']
        self.executed_by = executed_by

    def log(self , log_message):
        client = MongoClient(self.url)
        database = client[self.logger_database]
        connection = database[self.logger_collection]
        message = {
            'executed by ' : self.executed_by,
            'execution time' : datetime.now().strftime("%H:%M:%S"),
            'log message' : log_message
        }
        connection.insert(message)
