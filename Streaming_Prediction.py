import os

from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel
from utility import read_params
import SparkTransfomer
import Spark_Consumer

if __name__ == "__main__":
    spark_session = SparkTransfomer.SparkTransformer().SparkSessionObject()
    config_path = os.path.join("config","params.yaml")
    config1 = read_params(config_path=config_path)
    consumer = Spark_Consumer.Consumer(config=config1)
    transformer_list = []
    pipeline_model = PipelineModel.load(os.path.join("data","pipeline"))
    random_forest_model = RandomForestRegressionModel.load(os.path.join("data","model_path"))
    transformer_list.append(pipeline_model)
    transformer_list.append(random_forest_model)
    consumer.add_machine_learning_model(
        ml_models=transformer_list
    )
    consumer.consume()

