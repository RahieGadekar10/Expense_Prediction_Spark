import argparse
import os
import Data_Ingestion
import Data_Transformation
import Model_Training
import Model_Prediction
import SparkTransfomer
import AppLogger
from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel
from utility import read_params

class Complete :
    def __init__(self , config ,is_logger_enable = True, dataframe=None, pipeline_path = None):
        self.config = config
        self.dataframe = dataframe
        self.update_data = self.config['update_datatype']['update_data']
        self.target_column = self.config["target_columns"]['columns']
        self.columns_to_encode = self.config['transformation']['String_Indexer']['input_columns']
        self.logger_database = self.config['log_database']['log_training_database']
        self.logger_collection = self.config['log_database']['log_training_collection']
        self.is_logger_enable = is_logger_enable
        self.logger = AppLogger.AppLogger(self.config , self.logger_database , self.logger_collection)
        self.pipeline_path = self.config['pipeline_path']
        self.index_columns = self.config['transformation']['String_Indexer']['input_columns']
        self.required_columns = self.config['required_columns']['columns']
        self.spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
        self.prediction_file_path = self.config['prediction_data']['prediction_file']
        self.model_path = self.config['model_path']
        self.good_dir_path = self.config['training_data']['good_dir_path']
        self.training_file_path = self.config['training_data']['training_file_path']

    def ExecuteTrainer(self):
        ingest = Data_Ingestion.DataIngestion(config=self.config)
        ingest.read_file()
        self.dataframe= self.spark.read.csv(self.training_file_path , header=True , inferSchema=True)
        transformation = Data_Transformation.DataTransformation(config=self.config, dataframe=self.dataframe)
        train_df , test_df = transformation.transform()
        train_df.printSchema()
        trainer = Model_Training.ModelTrainer(config=self.config)
        trainer.get_model_results(train_df=train_df , test_df=test_df)

    def ExecutePredictor(self):
        dataframe = self.spark.read.csv(self.prediction_file_path, header=True , inferSchema=True)
        predict = Model_Prediction.ModelPrediction(config=self.config, dataframe=dataframe)
        predict.predict()

def main(config_path : str) :
    config1 = read_params(config_path=config_path)
    comp = Complete(config=config1)
    comp.ExecuteTrainer()
    comp.ExecutePredictor()

if __name__ == "__main__" :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default= os.path.join("config","params.yaml"))
    parsed_args = args.parse_args()
    main(config_path=parsed_args.config)