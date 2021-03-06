import json
import os
import time

from confluent_kafka import Consumer, KafkaError
from slack import WebClient
from slack.errors import SlackApiError


def error_cb(error):
    print(">>>", error)
    if error == KafkaError._ALL_BROKERS_DOWN:
        print("SERVER DOWN")


# Bot User OAuth Access Token
# used scopes: channels:history, channels:read, chat:write, im:history, mpim:history, users:read
token = os.environ["SLACK_BOT_TOKEN"]

sc = WebClient(token)

# Set 'auto.offset.reset': 'smallest' if you want to consume all messages
# from the beginning of the topic
settings = {
    "bootstrap.servers": os.environ["CLOUDKARAFKA_BROKERS"],
    "group.id": "%s-consumer" % os.environ["CLOUDKARAFKA_USERNAME"],
    "default.topic.config": {"auto.offset.reset": "largest"},
    "error_cb": error_cb,
    "session.timeout.ms": 6000,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "SCRAM-SHA-256",
    "sasl.username": os.environ["CLOUDKARAFKA_USERNAME"],
    "sasl.password": os.environ["CLOUDKARAFKA_PASSWORD"]
    # "debug": "broker, cgrp",
}
c = Consumer(settings)

# Topic = "AJOU-NOTIFY
c.subscribe([os.environ["CLOUDKARAFKA_TOPIC"]])

try:
    while True:
        msg = c.poll(1.0)
        time.sleep(1)
        if msg is None:
            continue
        elif not msg.error():
            print("Received a message: {0}".format(msg.value()))
            if msg.value() is None:
                print("But the message value is empty.")
                continue

            try:
                app_msg = json.loads(msg.value().decode())
            except:
                app_msg = json.loads(msg.value())

            title = app_msg["TITLE"]
            date = app_msg["DATE"]
            href = app_msg["LINK"]
            writer = app_msg["WRITER"]

            channel = "아주대"  # C01G2CR5MEE
            # TODO: 학사면 좀 더 중요하게?
            text = ":star: `%s` 새로운 공지!\n>%s: %s\n>링크: <%s|공지 확인하기>" % (
                date,
                writer,
                title,
                href,
            )
            print('\nSending message "%s" to channel %s' % (text, channel))

            try:
                sc_response = sc.chat_postMessage(
                    channel=channel, text=text, as_user=True, username="아주대 공지 봇"
                )  # as_user은 new slack app에서 작동 안 함

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

except KeyboardInterrupt:
    print("Pressed CTRL+C...")

finally:
    print("Closing...")
    c.close()
