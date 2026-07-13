from kafka import KafkaConsumer
import pandas as pd
import joblib
import atexit
import json


Kafka_Topic='financial-transactions'
Kafka_Server='localhost:9092'

consumer=KafkaConsumer(
    Kafka_Topic, 
    bootstrap_servers=Kafka_Server,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


parent_dir = '/home/victus/Fahad/DATA-Project/Transaction_Fraud_Detecter'

model_path = f"{parent_dir}/Model/Final/xgb_fraud_detection_model.pkl"
encoder_path = f"{parent_dir}/Model/Final/onehot_encoder.pkl"

# Load model & encoder
model = joblib.load(model_path)
encoder = joblib.load(encoder_path)


def detect_fraud_transactions():

    for message in consumer:

        txn = message.value
        txn_id='txn_'+str(txn['step'])+txn['nameOrig']+txn['nameDest']

        df = pd.DataFrame([txn])
        df = df.drop(["nameOrig", "nameDest"], axis=1)

        categorical_features = ["type"]
        numeric_cols = [c for c in df.columns if c not in categorical_features]

        encoded_cat = encoder.transform(df[categorical_features]).toarray()

        df_final = pd.concat([pd.DataFrame(encoded_cat), df[numeric_cols].reset_index(drop=True)], axis=1)

        is_Fraud = int(model.predict(df_final)[0])       # 0(False) OR 1(True)

        if is_Fraud==1:
            print(f'ALERT: Fraudulent transaction detected | Transaction_id : {txn_id}')


def close_fraud_detecter():
    consumer.close()
    print('Transaction Fraud Detecter closed')

atexit.register(close_fraud_detecter)

if __name__ == '__main__':
    detect_fraud_transactions()

