import json
import time


import os

from confluent_kafka import Consumer, KafkaError
from slack import WebClient
from slack.errors import SlackApiError


# Bot User OAuth Access Token
# Scope = chat:write
token = os.environ["SLACK_BOT_TOKEN"]

sc = WebClient(token)

# Set 'auto.offset.reset': 'smallest' if you want to consume all messages
# from the beginning of the topic
settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "kafka-notify",
    "default.topic.config": {"auto.offset.reset": "largest"},
}
c = Consumer(settings)

# Topic = "SLACK-KAFKA"
c.subscribe(["SLACK-KAFKA"])

# TODO: Make bolts with Apache Storm
try:
    while True:
        msg = c.poll(0.1)  # read data
        time.sleep(5)
        if msg is None:
            continue
        elif not msg.error():
            print("Received message: {0}".format(msg.value()))
            if msg.value() is None:
                continue

            try:
                app_msg = json.loads(msg.value().decode())
            except:
                app_msg = json.loads(msg.value())

            try:
                user = app_msg["USER"]
                message = app_msg["TEXT"]
                channel = "kafka"
                text = (
                    "`%s` found a bug :\n> %s\n\n_Please see if we can fix the issue *right here, right now*_"
                    % (user, message)
                )
                print('\nSending message "%s" to channel %s' % (text, channel))
            except SlackApiError as e:
                print("Failed to get channel/text from message.")
                print(e.response["error"])
                channel = "general"
                text = msg.value()

            try:
                sc_response = sc.chat_postMessage(channel=channel, text=text,)
            except SlackApiError as e:
                assert e.response["ok"] is False
                print("\t** FAILED: %s" % e.response["error"])
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print(
                "End of partition reached {0}/{1}".format(msg.topic(), msg.partition())
            )
        else:
            print("Error occured: {0}".format(msg.error().str()))

except Exception as e:
    print(type(e))
    print(dir(e))

finally:
    c.close()
