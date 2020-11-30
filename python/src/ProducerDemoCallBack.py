from confluent_kafka import Producer
from config import Config


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))  # binary


p = Producer({"bootstrap.servers": Config.MY_SERVER})

try:
    for val in range(1, 5):
        p.produce(Config.TOPIC_ID, "value #{0}".format(val), callback=acked)
        p.poll(0.5)

except KeyboardInterrupt:
    pass

p.flush(100)

# kafka-console-consumer --bootstrap-server localhost:9092 --topic first-topic
