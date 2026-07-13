from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow import Dataset
from pendulum import datetime, duration
import logging

default_args = {
    'owner': 'Fahad',
    'retries': 1,
    'retry_delay': duration(seconds=5)
}

training_dataset = Dataset("file:///home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter/Data/Training_data")


@dag(dag_id='Training_Data_Sensor_dag_v1',
    description='This is a Fraud_Detection_Model new training-data sensing Dag',
    default_args=default_args,
    start_date=datetime(2025, 12, 5, 2, tz='Asia/Kolkata'),
    tags=["Training-Data Sensor", "Machine Learning", "Project"],
    schedule='*/3 * * * *',
    max_active_runs=1,
    catchup=False,
    )
def workflow():

    New_Training_Data_Sensor=FileSensor(
        task_id='New_Training_Data_Sensor',
        filepath='home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter/Data/Training_data/*',
        poke_interval=30,
        timeout=60,  
        deferrable=True)

    @task(outlets=[training_dataset])
    def Model_Training_Triggerer():
        logging.info('Triggered Fraud_Detection_Model_Trainer Dag')

    Trigger_Fraud_Detection_Model_Training=Model_Training_Triggerer()


    New_Training_Data_Sensor>>Trigger_Fraud_Detection_Model_Training


Sensor_workflow=workflow()

