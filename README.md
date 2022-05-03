# Expense Prediction Using Spark
The Project is used to predict the expenses of an Adult based on the parameters such as age , sex, bmi , number of children, etc. Spark is used for data preprocessing and spark MLlib libraries are used for data transformation and model creation. Random-Forest Model is used to train on the given data. The complete logging of each step is done in MongoDB database. You can predict the data using batch csv file or Real-Time. The Real-Time predicted data is stored in MongoDB database. You can also upload the csv file of the data on the webpage to predict on your custom data and download the predicted csv file. 

## Requirements : 
- Create a MongoDB cluster 
- Enter the connection string of the MongoDB cluster along with username and password in config/params.yaml file.
- Install Airflow for scheduling

## Deploying Model 

- Download the github repository using : 
  ```bash
  HTTPS : https://github.com/RahieGadekar10/Expense_Prediction_Spark.git
  ```
  ```bash 
  SSH : git@github.com:RahieGadekar10/Expense_Prediction_Spark.git
  ```
  ```bash 
  Github CLI : gh repo clone RahieGadekar10/Expense_Prediction_Spark
  ```
- Install the dependencies using : 
    ```bash 
    Pip : !pip install -r requirement.txt
    ```
    ```bash
    - Conda : !conda install -r requirement.txt
    ```
- To train the model execute the following commands in sequence : 
 ```bash
python Data_Ingestion.py
python Data_Transformation.py
python Model_Training.py
```
- For Prediction
```bash
python Model_Prediction.py
```
- Run the trainer and predictor at once : 
 ```bash
python complete.py
```
## Real-Time Prediction
 ```bash
Start Zookeeper and Kafka Server
```
- Start Producer
 ```bash
spark-submit Spark_Producer.py 
```
- Start Consumer
 ```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  Spark_Consumer.py
```
## Scheduled Prediction
- DAG is present in user_processing.py
```bash
start airflow webserver
start airflow scheduler
Goto Airflow webserver
```
- DAG will be present by the name user_processing.
- Run/Schedule the DAG to execute prediction operation.

## Data Flow

<img src = "https://github.com/RahieGadekar10/Expense_Prediction_Spark/blob/e3280f1b0a97c1ac20bad868e990d11a512fcc1f/Capture.png"> </img>
