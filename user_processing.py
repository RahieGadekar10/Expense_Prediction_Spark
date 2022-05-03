import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from complete import Complete
from datetime import datetime

with DAG("user_processing" , schedule_interval="@daily" , start_date=datetime(2020,1,1) , catchup=False) as dag :
    filesensor = FileSensor(filepath="/home/bigdata/airflow/dags/prediction_file" , task_id="Checking_For_File" , poke_interval=15 , timeout=60*5 , mode='reschedule' , soft_fail=True)
    runbash = BashOperator(task_id = "Predicting" , bash_command="python /home/bigdata/airflow/dags/Model_Prediction.py")
    rundummy = DummyOperator(task_id ="Finish")

filesensor>>runbash>>rundummy
