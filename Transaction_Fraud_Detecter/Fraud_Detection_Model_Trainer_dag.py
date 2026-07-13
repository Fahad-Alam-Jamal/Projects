from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.jira.hooks.jira import JiraHook
from airflow import Dataset
from pendulum import datetime, duration
import logging
import sys
import re

parent_dir='/home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter'

sys.path.append(parent_dir)




def send_jira_notification(summary, description, issue_type="Task"):
    """Create a Jira issue using JiraHook connection"""
    try:
        hook = JiraHook(jira_conn_id='jira_connection')
        jira_client = hook.get_conn()
        issue_dict = {
            'project': {'key': 'FDMTP'},
            'summary': summary,
            'description': description,
            'issuetype': {'name': issue_type},
        }
        new_issue = jira_client.create_issue(fields=issue_dict)
        logging.info(f"Jira issue created: {new_issue['key']}")
    except Exception as e:
        logging.error(f"Failed to send Jira notification: {e}")



def notify_jira_failure(context):
    dag_id = context.get('dag').dag_id
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    exception = context.get('exception')
    log_url = task_instance.log_url
    log_url = re.sub(r"&base_date=[^&]+", "", log_url)
    
    summary = f"[Airflow Alert] Task {task_id} failed in DAG {dag_id}"
    description = f"DAG: {dag_id}\nTask: {task_id}\nExecution Date: {context.get('execution_date')}\nException: {exception}\nLog URL: {log_url}"
    
    send_jira_notification(
        summary=summary,
        description=description,
        issue_type="Bug"
    )




def Extract():
    import Training_data_extracter as tde
    from pathlib import Path
    import shutil


    t_path=tde.new_data_path()
    
    if t_path=='No new data batch':
        training_dir = Path(f"{parent_dir}/Data/Training_data")
        shutil.rmtree(training_dir)
        training_dir.mkdir(parents=True, exist_ok=True)
        open(f"{parent_dir}/Metadata/Last_Trained_data.txt", "w").close()

        logging.info("Batch check complete. Found 0 New Batch!")
        return False
    else:
        logging.info("Batch check complete. New Data Batch Found!")
        return {'new_data_path':t_path}

    
    
def Train_and_Validate(ti):
    import Model_Trainer_Validater as mtv

    data_path = ti.xcom_pull(task_ids='Extract_Data')['new_data_path']
    auc_score = mtv.train_and_validate(data_path)

    if auc_score >= 0.95:
        return True
    else:
        msg = f"Model AUC below threshold ({auc_score}). Model not saved."
        logging.info(msg)

        send_jira_notification(
            summary="[Model Trainer] Low AUC — Model Not Saved",
            description=f"AUC Score: {auc_score}\nModel not saved.\nData Path: {data_path}",
            issue_type="Bug"
        )
        return False




def Save():
    import shutil

    source_dir = f'{parent_dir}/Model/Temp'
    destination_dir = f'{parent_dir}/Model/Final'
    shutil.copytree(source_dir, destination_dir, dirs_exist_ok=True)
    model_path = f'{destination_dir}/xgb_fraud_detection_model.pkl'

    logging.info(f'Model saved at : {model_path}')


    send_jira_notification(
        summary="[Model Trainer] Model Successfully Saved",
        description=f"Model has been trained and saved successfully at:\n{model_path}",
        issue_type="Task"
    )

    
training_dataset = Dataset("file:///home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter/Data/Training_data")

with DAG(
    dag_id='Fraud-Detection-Model-Trainer_dag_V01',
    description='Dag for training a Transaction Fraud Detecting XGBoost Model',
    default_args = {
        'owner': 'Fahad',
        'retries': 1,
        'retry_delay': duration(minutes=1),
        'on_failure_callback': notify_jira_failure
},
    start_date=datetime(2025,5,1,11, tz='Asia/Kolkata'),
    tags=["Model Trainer", "Machine Learning", "Project"],
    schedule=[training_dataset],
    max_active_runs=1,
    catchup=False
) as dag:
    
    Extract_Data=ShortCircuitOperator(
        task_id='Extract_Data',
        python_callable=Extract
    )
    
    Train_and_Validate_Model=ShortCircuitOperator(
        task_id='Train_and_Validate_Model',
        python_callable=Train_and_Validate
    )
    
    Save_Model=PythonOperator(
    task_id='Save_Model',
    python_callable=Save
    )
    
    Extract_Data>>Train_and_Validate_Model>>Save_Model

