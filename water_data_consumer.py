from kafka.consumer import KafkaConsumer
from db import database

collection_name = "water_pollution_data"

collection = database[collection_name]

brokers = "localhost:9092"
topic = "case-project-water-data"

consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

def extract_data(resp):
    pollutants = resp["pollutants"]
    datetime = resp["datetime"]
    return {
        "datetime":datetime,
        "pH":pollutants["pH"]["value"],
        "sulphate":pollutants["sulphate"]["value"],
        "Solids":pollutants["Solids"]["value"],
        "Chloramines":pollutants["Chloramines"]["value"],
        "Conductivity":pollutants["Conductivity"]["value"],
        "Trihalomethanes":pollutants["Trihalomethanes"]["value"],
        "Organic_carbon":pollutants["Organic_carbon"]["value"]     
    }

for resp in consumer:
    data = extract_data(resp)
    collection.insert_one(data)
    print("*"*50,"water data inserted","*"*50)
