import os
from AppLogger import AppLogger
from pyspark.sql import SparkSession
import shutil
import os
import config
import mongo

class DataIngestion :
    def __init__(self, config , is_log_enabled=True):
        self.config = config
        self.logging_database = self.config['log_database']['log_training_database']
        self.logger_collection =  self.config['log_database']['log_training_collection']
        self.logger = AppLogger(self.config , self.logging_database , self.logger_collection)
        self.good_dir_path = self.config['training_data']['good_dir_path']
        self.training_file_path = self.config['training_data']['training_file_path']
        self.is_log_enabled = is_log_enabled

    def read_file(self):
        self.logger.log("Data Ingestion Process Started")
        dir_path = self.good_dir_path
        self.logger.log("Reading Files from Directory")
        file = os.listdir(dir_path)
        for file in file:
            if file.endswith(".csv") :
                file_path = os.path.join(dir_path,file)
                self.logger.log("Moving csv Files to Training Directory")
                shutil.move(file_path , self.training_file_path)
            else :
                continue



