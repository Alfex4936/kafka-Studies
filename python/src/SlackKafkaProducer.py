import json
import time

from config import Config
from confluent_kafka import Producer
from slack import WebClient
from slack.errors import SlackApiError


# Bot User OAuth Access Token
# used scopes: channels:history, channels:read, chat:write, im:history, mpim:history, users:read
token = ""

# Slack API 초기화
sc = WebClient(token)

# Kafka Producer 만들기  "localhost:9092"
settings = {"bootstrap.servers": Config.MY_SERVER}
p = Producer(settings)


def acked(err, msg):  # callback
    if err is not None:
        print("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))  # binary


channel = "C01FVD0QD42"  # 아래 sc.conversations_list로 id를 확인


# channel_name = "일반"
# try:
#     sc_response = sc.conversations_list(channel=channel)
#     # for channel in sc_response["channels"]:
#     #     print(channel["name"])
#     # if channel["name"] == channel_name:
#     #     channel_id = channel["id"]
# except SlackApiError as e:
#     assert e.response["ok"] is False
#     print("\t** FAILED: %s" % e.response["error"])


posts = {}  # 켤 때마다 중복 메시지 받음, 파일에 저장하는 형식으로 하면 더 나음.

# 매 5초마다 메시지를 계속 읽어옴.
# ratelimited 에러가 발생하면, 시간대를 늘려야 함.
try:
    time.sleep(5)
    while True:
        try:
            sc_response = sc.conversations_history(channel=channel)
            for msg in sc_response["messages"]:
                if msg["ts"] not in posts:  # 없는 메시지
                    posts[msg["ts"]] = True
                    if "bug" in msg["text"].lower():  # bug를 포함한 글임
                        print("Someone posted a bug...")
                        name = sc.users_info(user=msg["user"])["user"][
                            "name"
                        ]  # user id를 name으로 변환
                        data = {"USER": name, "TEXT": msg["text"]}

                        # 데이터 Consumer에게 전송
                        p.produce(
                            Config.SLACK_TOPID_ID,
                            value=json.dumps(data),
                            callback=acked,
                        )
                        p.poll(0.5)
                    else:
                        # 파일에 저장할 수도
                        continue
        except SlackApiError as e:
            assert e.response["ok"] is False
            print("\t** FAILED: %s" % e.response["error"])


except Exception as e:
    print(type(e))
    print(dir(e))

finally:
    print("Exiting...")
    p.flush(100)
