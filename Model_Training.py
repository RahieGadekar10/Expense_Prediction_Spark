from pyspark.ml.regression import RandomForestRegressor
from sklearn.metrics import r2_score , mean_squared_error
import math
import Data_Transformation
import AppLogger
import argparse
import os
from utility import read_params
import SparkTransfomer

class ModelTrainer:
    def __init__(self, config, is_log_enabled = True, dataframe = None):
        self.config = config
        self.logger_database = self.config['log_database']['log_training_database']
        self.logger_collection = self.config['log_database']['log_training_collection']
        self.is_log_enabled = is_log_enabled
        self.logger = AppLogger.AppLogger(self.config , self.logger_database , self.logger_collection)
        self.pipeline_path = config['pipeline_path']
        self.dataframe = dataframe
        self.spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
        self.model_path = self.config['model_path']
        self.good_dir_path = self.config['training_data']['good_dir_path']
        self.training_file_path = self.config['training_data']['training_file_path']
        self.transform = Data_Transformation.DataTransformation(config=self.config, dataframe=self.dataframe)

    def r2_score_metric(self, y_true , y_pred) :
        return r2_score(y_true , y_pred)

    def root_mean_squared_error(self, y_true , y_pred) :
        return math.sqrt(mean_squared_error(y_true , y_pred))

    def get_model_results(self,train_df , test_df) :
        rf = RandomForestRegressor(featuresCol="features" , labelCol="expenses")
        model = rf.fit(train_df)
        self.logger.log("Sucessfully Fit the RandomForest Model")
        train_prediction = model.transform(train_df)
        self.logger.log("Train Prediction done")
        test_prediction = model.transform(test_df)
        self.logger.log("Test Prediction Done")
        training_data = train_prediction.select("expenses" , "prediction").toPandas()
        testing_data = test_prediction.select("expenses","prediction").toPandas()

        train_r2_score = self.r2_score_metric(training_data['expenses'] , training_data['prediction'])
        self.logger.log(f"R2 Score for training is : {train_r2_score}")
        train_rmse = self.root_mean_squared_error(training_data['expenses'], training_data['prediction'])
        self.logger.log(f"RMSE Score for training is : {train_rmse}")

        test_r2_score = self.r2_score_metric(testing_data['expenses'], testing_data['prediction'])
        self.logger.log(f"R2 Score for testing is : {test_r2_score}")
        test_rmse = self.root_mean_squared_error(testing_data['expenses'], testing_data['prediction'])
        self.logger.log(f"RMSE Score for testing is : {test_rmse}")

        model.write().overwrite().save(self.model_path)
        self.logger.log("Model Saved Successfully")

def main(config_path:str) :
    config1 = read_params(config_path)
    spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
    df = spark.read.csv("TrainingFile\master.csv", header=True, inferSchema=True)
    mt = ModelTrainer(config=config1, dataframe=df)
    mt.get_model_results()

if __name__=='__main__' :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default=os.path.join("config","params.yaml"))
    parsed_args =args.parse_args()
    main(config_path=parsed_args.config)
