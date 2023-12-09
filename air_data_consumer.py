from kafka.consumer import KafkaConsumer

brokers = "localhost:9092"
topic = "case-project-air-data"

consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

for msg in consumer:
    print(msg)
    print("*"*50)
