from kafka.producer import KafkaProducer
import requests
import json
import time

brokers = "localhost:9092"
water_data_topic = "case-project-water-data"
air_data_topic = "case-project-air-data"

producer = KafkaProducer(bootstrap_servers=brokers)


def get_data(topic, api_url):

    response = requests.get(api_url).json()
    data = json.dumps(response['results']).encode('utf-8')

    producer.send(topic, data)
    print(f"Produced messae on topic: {topic}")
    producer.flush()

def start_producer():
    count = 0
    while count < 15:
        get_data(topic=water_data_topic, api_url="https://randomuser.me/api")
        get_data(topic=air_data_topic, api_url="https://randomuser.me/api")

        time.sleep(10)
        count += 1


if __name__ == "__main__":
    start_producer()
