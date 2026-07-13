##  cd ~/Fahad/Kafka/App_Traffic_Monitor

##  source /home/victus/Python-3/bin/activate

##  python Usage_Reporter.py



from kafka import KafkaProducer
import requests
import atexit
import json


Kafka_Topic='app-events'
Kafka_Server='localhost:9092'

producer=KafkaProducer(bootstrap_servers=Kafka_Server)


def get_usage_data():

    # API endpoint
    url = "https://fakerapi.it/api/v2/custom"
    params = {
        "_quantity": 1,
        "_locale": "en_IN",
        "user_id": "ean",
        "state": "state",
        "event_id": "buildingNumber",
        "device_id": "boolean_digit"
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        raw_event = data.get("data", [])
        if raw_event and isinstance(raw_event, list) and len(raw_event) > 0:
            raw_event=raw_event[0]
            user_id = int(raw_event["user_id"][0:3])
            event_id = int(raw_event["event_id"][0:1])
            event_id = event_id-5 if event_id>5 else event_id
            event = {
                "user_id": user_id,
                "state": raw_event["state"],
                "event_id": event_id,
                "device_id": raw_event["device_id"]
            }
            return event
        else:
            return None
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None


def report_usage():
    print(f'Sending app usage data to Kafka topic {Kafka_Topic}')

    while True:
        event = get_usage_data()
        if event:
            event = json.dumps(event)
            producer.send(Kafka_Topic, event.encode('utf-8'))
        else:
            pass


def stop_reporter():
    producer.flush()
    producer.close()
    print('Reporter Stopped')

atexit.register(stop_reporter)


if __name__ == '__main__':
    report_usage()

