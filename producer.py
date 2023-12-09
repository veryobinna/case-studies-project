from kafka.producer import KafkaProducer
import requests
import json
import time

from my_secrets import water_data_api_key, air_data_api_key


LONGITUDE = "55.4920"
LATITUDE = "4.6796"
AIR_DATA_API_URL = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={LATITUDE}&lon={LONGITUDE}&appid={air_data_api_key}"
WATER_DATA_API_URL = f"https://api.meersens.com/environment/public/water/current?lat={LATITUDE}&lng={LONGITUDE}"

BROKERS = "localhost:9092"
WATER_DATA_TOPIC = "case-project-water-data"
AIR_DATA_TOPIC = "case-project-air-data"

producer = KafkaProducer(bootstrap_servers=BROKERS)


def get_data(topic, api_url):

    response = requests.get(api_url).json()
    data = json.dumps(response['results']).encode('utf-8')

    producer.send(topic, data)
    print(f"Produced messae on topic: {topic}")
    producer.flush()

def start_producer():
    count = 0
    while count < 15:
        get_data(topic=WATER_DATA_TOPIC, api_url=WATER_DATA_API_URL, headers={"apikey": water_data_api_key})
        get_data(topic=AIR_DATA_TOPIC, api_url=AIR_DATA_API_URL)

        time.sleep(10)
        count += 1


if __name__ == "__main__":
    start_producer()
