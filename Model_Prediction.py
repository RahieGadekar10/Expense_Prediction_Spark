import Data_Transformation
import AppLogger
import argparse
import os
from utility import read_params
import SparkTransfomer
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml import PipelineModel

class ModelPrediction:
    def __init__(self, config, is_log_enabled = True, dataframe = None):
        self.config = config
        self.logger_collection = self.config['log_database']['log_prediction_collection']
        self.logger_database = self.config['log_database']['log_prediction_database']
        self.is_log_enabled = is_log_enabled
        self.logger = AppLogger.AppLogger(self.config , self.logger_database , self.logger_collection)
        self.pipeline_path = config['pipeline_path']
        self.dataframe = dataframe
        self.spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
        self.model_path = self.config['model_path']
        self.prediction_file_path = self.config['prediction_data']['prediction_file']

    def predict(self):
        pipeline_model = PipelineModel(self.pipeline_path)
        self.logger.log("Reading Data...")
        self.dataframe = self.spark.read.csv(self.prediction_file_path , header= True , inferSchema=True)
        self.logger.log("Data Read Successfully")
        transformation = Data_Transformation.DataTransformation(self.config,dataframe=self.dataframe,pipeline_path=pipeline_model)
        self.dataframe= transformation.predictor_transform()
        self.logger.log("Transformed Data Successfully")
        self.logger.log("Initialising the Machine Learning Model")
        model = RandomForestRegressionModel.load(self.model_path)
        self.logger.log("Model loaded successfully")
        self.logger.log("Predicting results...")
        pred = model.transform(self.dataframe)
        self.logger.log("Saving Predictions in csv file")
        prediction_output = pred.select("age", "sex", "children", "smoker", "prediction").toPandas()
        prediction_output.to_csv("prediction_output\output.csv" , header = prediction_output.columns , index= None)
        self.logger.log("Predictions saved successfully")
        self.spark.stop()

def main(config_path : str) :
    config1 = read_params(config_path=config_path)
    modelpred = ModelPrediction(config=config1)
    modelpred.predict()

if __name__ == "__main__" :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default= os.path.join("config","params.yaml"))
    parsed_args = args.parse_args()
    main(config_path=parsed_args.config)