from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration
import subprocess
import logging
import os


def Data_Extracter(table_name):
    from pyspark.sql import SparkSession

    spark=SparkSession.builder.appName("Data_Extracter") \
        .config("spark.jars", "/home/victus/JBDC_Drivers/mysql-connector-j-8.2.0.jar").getOrCreate()

    data_path='/home/victus/Fahad/DATA-Project/Ecommerce_Project/Real_time-Raw_data/'

    r_df=spark.read.csv(f'{data_path}{table_name}.csv',header=True,inferSchema=True)
    
    s_df=spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/Ecommerce_DW_Staging_Area",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=table_name,
    user="root",
    password="password").load()

    data=r_df.subtract(s_df)

    data_count=data.count()

    if data_count!=0:
        data.write.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/Ecommerce_DW_Staging_Area",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=table_name,
        user="root",
        password="password").mode("overwrite").save()

        logging.info(f'{table_name.title()} Data Extracted')
    else:
        logging.info(f'No New {table_name.title()}')


def Data_Loder():
    from pyspark.sql import SparkSession

    spark=SparkSession.builder \
        .appName("DataWarehouse_Populator") \
        .config("spark.jars", "/home/victus/JBDC_Drivers/mysql-connector-j-8.2.0.jar,/home/victus/JBDC_Drivers/clickhouse-jdbc-0.8.5-all.jar") \
        .getOrCreate()

    for i in ['users','products','orders','clickstream']:

        if i=='clickstream':
            c_table='fact_clicks'
        elif i=='orders':
            c_table='fact_'+i
        else:
            c_table='dim_'+i

        s_table='T'+i

        # Read from Staging_Area
        mysql_df = spark.read.format("jdbc").options(
            url="jdbc:mysql://localhost:3306/Ecommerce_DW_Staging_Area",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=s_table,
            user="root",
            password="password"
        ).load()


        # Append to DataWarehouse
        mysql_df.write.format("jdbc").options(
            url="jdbc:clickhouse://localhost:8123/Ecommerce_DW",
            driver="com.clickhouse.jdbc.ClickHouseDriver",
            dbtable=c_table,
            user="default",
            password="password"
        ).mode("append").save()

    subprocess.run([
    os.path.expanduser("~/Python-3/bin/python"),
    os.path.expanduser("~/Fahad/DATA-Project/Ecommerce_Project/Data_Generator.py")])

    logging.info('Data Appended in DataWarehouse')
        

default_args = {
    'owner': 'Fahad',
    'retries': 2,
    'retry_delay': duration(minutes=1)
}

with DAG(
    dag_id='Ecommerce_data_ETL_v01',
    description='This is a Ecommerce data ETL Dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 4, 2, tz='Asia/Kolkata'),
    tags=["Ecommerce", "ETL", "dbt", "Project"],
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:
    
    Extract_UserData=PythonOperator(
        task_id="Extract_UserData",
        python_callable=Data_Extracter,
        op_kwargs={'table_name':'users'}
    )

    Extract_OrderData=PythonOperator(
        task_id="Extract_OrderData",
        python_callable=Data_Extracter,
        op_kwargs={'table_name':'orders'}
    )

    Extract_ProductData=PythonOperator(
        task_id="Extract_ProductData",
        python_callable=Data_Extracter,
        op_kwargs={'table_name':'products'}
    )

    Extract_ClickstreamData=PythonOperator(
        task_id="Extract_ClickstreamData",
        python_callable=Data_Extracter,
        op_kwargs={'table_name':'clickstream'}
    )

    Transform=BashOperator(
        task_id='Transform',
        bash_command='''source ~/Python-3/bin/activate && \
        cd ~/Fahad_dbt/Ecommerce_project_dbt && \
        dbt run'''
    )

    Load=PythonOperator(
        task_id='Load',
        python_callable=Data_Loder
    )

    [Extract_UserData, Extract_ProductData, Extract_ClickstreamData, Extract_OrderData]>>Transform>>Load


