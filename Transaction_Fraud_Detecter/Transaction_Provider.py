from kafka import KafkaProducer
import pandas as pd
import atexit
import json


Kafka_Topic='financial-transactions'
Kafka_Server='localhost:9092'

producer=KafkaProducer(
    bootstrap_servers=Kafka_Server,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


def transaction_provider():

    df=pd.read_parquet('/home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter/Data/transactions.parquet')

    print('Sending Transactions for Fraud Detection....')

    for _ , row in df.iterrows():
        producer.send(Kafka_Topic, row.to_dict())


def close_provider():
    producer.flush()
    producer.close()
    print('Transaction Provider closed')

atexit.register(close_provider)


if __name__ == '__main__':
    transaction_provider()

