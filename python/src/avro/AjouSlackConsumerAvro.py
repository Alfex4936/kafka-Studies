import os
import time
from io import BytesIO

from confluent_kafka import Consumer, KafkaError, KafkaException
from fastavro import parse_schema, schemaless_reader, writer
from slack import WebClient
from slack.errors import SlackApiError


schema = {  # avsc
    "namespace": "ajou.parser",
    "name": "Notice",  # seperated but will be namespace.name
    "doc": "A notice parser from Ajou university.",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "title", "type": "string"},
        {"name": "date", "type": "string"},
        {"name": "link", "type": "string"},
        {"name": "writer", "type": "string"},
    ],
}
parsed_schema = parse_schema(schema)


# Bot User OAuth Access Token
# used scopes: channels:history, channels:read, chat:write, im:history, mpim:history, users:read
token = os.environ["SLACK_BOT_TOKEN"]

sc = WebClient(token)

# Set 'auto.offset.reset': 'smallest' if you want to consume all messages
# from the beginning of the topic
settings = {
    "bootstrap.servers": os.environ["VM_SERVER"],
    "group.id": "ajou-notify",
    "default.topic.config": {"auto.offset.reset": "largest"},
    # "value.deserializer": lambda v: json.loads(v.decode("utf-8")),
    # "debug": "broker, cgrp",
}
c = Consumer(settings)

# Topic = "AJOU-NOTIFY
c.subscribe(["AJOU-NOTIFY"])

try:
    while True:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = c.poll(1.0)
        if msg is None:
            time.sleep(10)
            continue
        elif not msg.error():
            print("Received messages: {0}".format(len(msg)))
            if msg.value() is None:
                print("But the message value is empty.")
                continue

            # Consumer read message
            message = msg.value()
            rb = BytesIO(message)

            app_msg = schemaless_reader(rb, parsed_schema)  # read one record
            try:
                title = app_msg["title"]
                date = app_msg["date"]
                href = app_msg["link"]
                writer = app_msg["writer"]

                channel = "아주대"  # C01G2CR5MEE
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
        elif msg.error():
            raise KafkaException(msg.error())
        else:
            print("Error occured: {0}".format(msg.error().str()))
except Exception as e:
    print(e)
    print(dir(e))

except KeyboardInterrupt:
    print("Pressed CTRL+C...")

finally:
    print("Closing...")
    c.close()
