import json
import time

import os

from config import Config
from confluent_kafka import Consumer, KafkaError
from slack import WebClient
from slack.errors import SlackApiError


# Bot User OAuth Access Token
token = os.environ["SLACK_BOT_TOKEN"]

sc = WebClient(token)

# Set 'auto.offset.reset': 'smallest' if you want to consume all messages
# from the beginning of the topic
settings = {
    "bootstrap.servers": Config.MY_SERVER,
    "group.id": "ajou-notify",
    "default.topic.config": {"auto.offset.reset": "largest"},
}
c = Consumer(settings)

# Topic = "AJOU-NOTIFY
c.subscribe([Config.AJOU_TOPIC_ID])

try:
    while True:
        msg = c.poll(0.1)
        time.sleep(5)
        if msg is None:
            time.sleep(10)
            continue
        elif not msg.error():
            print("Received a message: {0}".format(msg.value()))
            if msg.value() is None:
                continue

            try:
                app_msg = json.loads(msg.value().decode())
            except:
                app_msg = json.loads(msg.value())

            try:
                title = app_msg["TITLE"]
                date = app_msg["DATE"]
                href = app_msg["LINK"]
                writer = app_msg["WRITER"]

                channel = "아주대"
                # TODO: 학사면 좀 더 중요하게?
                text = ":star: `%s` 새로운 공지!\n>%s: %s\n>링크: <%s|공지 확인하기>" % (
                    date,
                    writer,
                    title,
                    href,
                )
                print('\nSending message "%s" to channel %s' % (text, channel))
            except SlackApiError as e:
                print("Failed to get channel/text from message.")
                print(e.response["error"])
                channel = "kafka"
                text = msg.value()

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
    c.close()
