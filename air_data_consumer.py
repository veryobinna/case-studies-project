from kafka.consumer import KafkaConsumer
from db import database

collection_name = "water_pollution_data"

collection = database[collection_name]

brokers = "localhost:9092"
topic = "case-project-air-data"

consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

def extract_data(resp):
    components = resp["components"]
    components["datetime"] = resp["dt"]
    return components
    

for resp in consumer:
    data = extract_data(resp)
    collection.insert_one(data)
    print("*"*50,"air data inserted","*"*50)
