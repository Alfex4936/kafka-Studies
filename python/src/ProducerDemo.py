from confluent_kafka import Producer
from config import Config

p = Producer({"bootstrap.servers": Config.MY_SERVER})
p.produce(Config.TOPIC_ID, key="key_1", value="Hello")
p.flush(100)

# kafka-console-consumer --bootstrap-server localhost:9092 --topic first-topic
