from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from pendulum import datetime, duration
from pathlib import Path
import logging
import json
import yaml


default_args = {
    'owner': 'Fahad',
    'retries': 1,
    'retry_delay': duration(minutes=1)
}

config_folder = Path("/home/victus/Fahad/DATA-Project/Amazon_Ecommerce_ELT/country_data_config")


@dag(dag_id='Ecommerce_data_ELT_v01',
    description='This is a Amazon Ecommerce data ELT Dag',
    default_args=default_args,
    start_date=datetime(2025, 12, 5, 2, tz='Asia/Kolkata'),
    tags=["Amazon", "Ecommerce", "ELT", "Project"],
    schedule='*/5 * * * *',
    catchup=False,
    max_active_runs=1
)
def ELT():

    @task.short_circuit
    def Extract_amazon_orders():
        s3_hook = S3Hook(aws_conn_id="ID_S3_airflow")
        hook = HttpHook(
            method='GET',
            http_conn_id='ID_cloud_server'
        )

        conn = hook.get_connection(hook.http_conn_id)
        headers = {"apidogToken": conn.password}
        
        for file in config_folder.glob("*.yaml"):
            with open(file, "r") as f:
                config = yaml.safe_load(f)

            response = hook.run(
                endpoint=config['endpoint'],
                headers=headers
            )
            data = response.json()[config['datakey']]
            sdata = json.dumps(data)
            s3_hook.load_string(
                string_data=sdata,
                bucket_name=config['s3_storage']['bucket'],
                key=f'New-Orders/{config['s3_storage']['key']}',
                replace=True
            )

            if response.status_code != 200:
                return False
            
        logging.info('New orders stored in s3://amazon-orders/New-Orders/')
        return True


    @task
    def Load(ts_nodash):
        import pandas as pd
        import tempfile
        import shutil

        s3_hook = S3Hook(aws_conn_id="ID_S3_airflow")

        for file in config_folder.glob("*.yaml"):
            with open(file, "r") as f:
                config = yaml.safe_load(f)

            source_bucket = config['s3_storage']['bucket']
            source_key = f'New-Orders/{config['s3_storage']['key']}'

            target_bucket = config['s3_storage']['bucket']
            target_key = fr'Raw-Orders/orders_at_{ts_nodash}/{config['datakey']}.parquet'

            local_dir = tempfile.mkdtemp()
            local_parquet = f'{local_dir}/output.parquet'


            local_json = s3_hook.download_file(
                key=source_key,
                bucket_name=source_bucket,
                local_path=local_dir
            )

            df = pd.read_json(local_json)
            df.to_parquet(local_parquet, engine="pyarrow", index=False)

            s3_hook.load_file(
                filename=local_parquet,
                key=target_key,
                bucket_name=target_bucket,
                replace=True
            )

            shutil.rmtree(local_dir)

        logging.info('New orders stored in Data Lake (S3)')


    @task
    def Transform():
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col,lit,replace,split,trim,concat,to_timestamp
        import os

        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")

        spark = SparkSession.builder \
            .appName("Transform") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.jars", "/home/victus/JBDC_Drivers/clickhouse-jdbc-0.8.5-all.jar") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint","http://localhost:9002") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        
        mschema = """
            country string,
            `order-id` string,
            product string,
            price_dollar float,
            `payment method` string,
            rating int,
            comment string,
            `delivery address` string,
            timestamp timestamp
        """

        df = spark.createDataFrame([], schema=mschema)

        for file in config_folder.glob("*.yaml"):
            with open(file, "r") as f:
                config = yaml.safe_load(f)
        
            df_s = spark.read.json(f's3a://{config['s3_storage']['bucket']}/New-Orders/{config['s3_storage']['key']}')
            df_s = df_s.withColumn('country', split(col('delivery address'), lit(','))[3])\
                .withColumn('country', trim(col('country')))\
                .withColumn('price', replace(col('price'), lit('$'), lit('')))\
                .withColumn('price_dollar', col('price').cast('float'))\
                .withColumn('rating', col('rating').cast('int'))\
                .withColumn('timestamp', to_timestamp(concat(col('date'),lit(' '), col('time'))))
            
            df_s = df_s.select('country','order-id','product','price_dollar','payment method','rating','comment','delivery address','timestamp')

            df = df.union(df_s)

        df.write.format("jdbc").options(
            url=f"jdbc:clickhouse://localhost:8123/{config['table_storage']['database']}",
            driver="com.clickhouse.jdbc.ClickHouseDriver",
            dbtable=config['table_storage']['table'],
            user="default",
            password=config['table_storage']['password']
        ).mode("append").save()

        logging.info('Amazon_Orders_DW (ClickHouse) is Updated')


    Extract_amazon_orders=Extract_amazon_orders()
    Load=Load()
    Transform=Transform()

    Extract_amazon_orders>>Load>>Transform


Workflow=ELT()

