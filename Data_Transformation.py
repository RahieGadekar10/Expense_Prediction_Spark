import argparse
import os.path
import yaml
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.ml.feature import StringIndexer , OneHotEncoder , VectorAssembler
import SparkTransfomer
from pyspark.ml import Pipeline
from utility import remove_unwanted_columns , read_params
import AppLogger

class DataTransformation :
    def __init__(self , config ,is_logger_enable = True, dataframe=None, pipeline_path = None):
        self.config = config
        self.dataframe = dataframe
        self.stages=[]
        self.target_column = self.config["target_columns"]['columns']
        self.columns_to_encode = self.config['transformation']['String_Indexer']['input_columns']
        self.logger_database = self.config['log_database']['log_training_database']
        self.logger_collection = self.config['log_database']['log_training_collection']
        self.is_logger_enable = is_logger_enable
        self.logger = AppLogger.AppLogger(self.config , self.logger_database , self.logger_collection)
        self.pipeline_path = self.config['pipeline_path']
        self.index_columns = self.config['transformation']['String_Indexer']['input_columns']
        self.required_columns = self.config['required_columns']['columns']

    def update_datatype(self, datatypes:dict):
        if not self.dataframe is None :
            for col , data in datatypes.items() :
                df = self.dataframe.withColumn(col , self.dataframe[col].cast(data))
                df.printSchema()
                self.logger.log("DataFrame Schema Updated")
                return df

    def Indexer(self, input_columns : list ) :
        stringindexer = StringIndexer(inputCols=input_columns , outputCols=[f"{column}_encoder" for column in input_columns])
        self.stages.append(stringindexer)
        self.logger.log("String Indexer Operation Completed")

        onehotencoder = OneHotEncoder(inputCols=stringindexer.getOutputCols() , outputCols=[f"{column}_encoded" for column in input_columns], dropLast=True)
        self.stages.append(onehotencoder)
        self.logger.log("One Hot Encoding Completed")

    def Assembler(self, required_columns : list) :
        vectorassembler = VectorAssembler(inputCols=required_columns , outputCol='features')
        self.stages.append(vectorassembler)
        self.logger.log("Vector Assembler Function Executed")

    def train_test_data(self, test_size=0.2):
        train_df, test_df = self.dataframe.randomSplit([1 - test_size, test_size], seed=0)
        self.logger.log("Train Test Split Done")
        return train_df, test_df

    def transform(self):
        update_data = {
            'age': IntegerType(),
            'sex': StringType(),
            'bmi': FloatType(),
            'children': IntegerType(),
            'smoker': StringType(),
            'region': StringType(),
            'expenses': FloatType()
        }
        self.update_datatype(update_data)
        self.Indexer(input_columns=self.index_columns)
        self.Assembler(required_columns=self.required_columns)
        pipe = Pipeline(stages=self.stages)
        pipeline_fit = pipe.fit(self.dataframe)
        self.dataframe = pipeline_fit.transform(self.dataframe)
        pipeline_fit.write().overwrite().save(self.pipeline_path)
        return self.train_test_data()

    def predictor_transform(self,):
        update_data = {
            'age': IntegerType(),
            'sex': StringType(),
            'bmi': FloatType(),
            'children': IntegerType(),
            'smoker': StringType(),
            'region': StringType()
        }
        self.update_datatype(update_data)
        self.Indexer(input_columns=self.index_columns)
        self.Assembler(required_columns=self.required_columns)
        pipe = Pipeline(stages=self.stages)
        pipeline_fit = pipe.fit(self.dataframe)
        self.dataframe = pipeline_fit.transform(self.dataframe)
        return self.dataframe

def main(config_path:str) :
    spark = SparkTransfomer.SparkTransformer().SparkSessionObject()
    df = spark.read.csv("D:\Big Data\spark_project-main\spark_project-main\Good_File_Path\HealthPrem_26092020_131534.csv", header=True, inferSchema=True)
    config = read_params(config_path)
    datatransformer = DataTransformation(config = config, dataframe=df)
    train_df , testdf = datatransformer.transform()
    train_df.show()
    spark.stop()

if __name__ == '__main__' :
    args = argparse.ArgumentParser()
    args.add_argument("--config" , default= os.path.join("config","params.yaml"))
    parsed_args = args.parse_args()
    print(parsed_args.config)
    main(config_path=parsed_args.config)

