from confluent_kafka import Producer
from config import Config
from contextlib import contextmanager


@contextmanager
def prod(settings):
    p = Producer(settings)
    yield p
    p.flush(100)


settings = {"bootstrap.servers": Config.MY_SERVER}

with prod(settings) as p:
    p.produce(Config.TOPIC_ID, key="key_1", value="Hello")

# kafka-console-consumer --bootstrap-server localhost:9092 --topic first-topic
