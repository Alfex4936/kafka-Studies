import datetime
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from config import Config
from confluent_kafka import Producer
from slack import WebClient
from slack.errors import SlackApiError

# Producer callback function
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}".format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))  # binary


# Make data into dictionary format
def makeJson(postId, postTitle, postDate, postLink, postWriter):
    duplicate = "[" + postWriter + "]"
    if duplicate in postTitle:  # writer: [writer] title
        postTitle = postTitle.replace(duplicate, "").strip()  # -> writer: title
    return {
        postId: {
            "TITLE": postTitle,
            "DATE": postDate,
            "LINK": ADDRESS + postLink,
            "WRITER": postWriter,
        }
    }


# Ajou notices parser
def parser():
    req = requests.get(f"{ADDRESS}?mode=list&&articleLimit=10&article.offset=0")
    req.encoding = "utf-8"
    html = req.text
    soup = BeautifulSoup(html, "html.parser")
    ids = soup.select("table > tbody > tr > td.b-num-box")
    posts = soup.select("table > tbody > tr > td.b-td-left > div > a")
    dates = soup.select("table > tbody > tr > td.b-td-left > div > div > span.b-date")
    writers = soup.select(
        "table > tbody > tr > td.b-td-left > div > div.b-m-con > span.b-writer"
    )
    return ids, posts, dates, writers


ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "already_read.json")
LENGTH = 10
PRODUCED = 0
DUMP = lambda x: json.dumps(x)
LOAD = lambda x: json.load(x)

# Bot User OAuth Access Token
# used scopes: channels:history, channels:read, chat:write, im:history, mpim:history, users:read
token = os.environ["SLACK_BOT_TOKEN"]

read = None
# 공지 Parser
if not Path(JSON_PATH).is_file():  # 파일 없으면 기본 형식 만듬
    base_data = {"POSTS": {}, "LAST_PARSED": "1972-12-01 07:00:00.000000"}

    with open(JSON_PATH, "a+") as f:
        f.write(DUMP(base_data))

# json read
with open(JSON_PATH, "r+") as f_read:
    read = LOAD(f_read)

# Set last parsed time to rest 1 hour well
LAST_PARSED = datetime.datetime.strptime(read["LAST_PARSED"], "%Y-%m-%d %H:%M:%S.%f")

# init parser
ids, posts, dates, writers = parser()

# Slack API 초기화
sc = WebClient(token)
channel = "C01G2CR5MEE"  # 아주대

# Kafka Producer 만들기  "localhost:9092"
settings = {"bootstrap.servers": Config.MY_SERVER}
p = Producer(settings)

try:
    while True:  # 1시간마다 무한 반복
        PRODUCED = 0
        LAST_PARSED = datetime.datetime.strptime(
            read["LAST_PARSED"], "%Y-%m-%d %H:%M:%S.%f"
        )

        try:
            now = datetime.datetime.now()
            diff = (now - LAST_PARSED).seconds
            print("Last parsing:", LAST_PARSED)
            if diff / 3600 < 1:  # 업데이트 후 1시간이 안 지났음, 대기
                print(f"Wait for {3600 - diff} seconds to sync new posts.")
                time.sleep(3600 - diff)

            read["LAST_PARSED"] = now.strftime("%Y-%m-%d %H:%M:%S.%f")

            print("Trying to parse new posts...")
            ids, posts, dates, writers = parser()  # 다시 파싱
            for i in range(LENGTH):
                postId = ids[i].text.strip()
                postLink = posts[i].get("href")
                postTitle = posts[i].text.strip()
                # postTitle = posts[i].get("title")
                postDate = dates[i].text.strip()
                postWriter = writers[i].text

                data = makeJson(postId, postTitle, postDate, postLink, postWriter)
                # {'10000': {'TITLE': '설문조사', 'DATE': '20.12.04', 'LINK': 'https', 'WRITER': '입학처'}}

                if postId not in read["POSTS"]:
                    print("Sending a new post...:", postId)
                    read["POSTS"].update(data)

                    PRODUCED += 1
                    p.produce(
                        Config.AJOU_TOPIC_ID, value=DUMP(data[postId]), callback=acked,
                    )
                    p.poll(0.5)  # 데이터 Kafka에게 전송
                else:
                    continue
            if PRODUCED:
                print(f"Sent {PRODUCED} posts...")
            else:
                print("No new posts yet...")

        except SlackApiError as e:
            assert e.response["ok"] is False
            print("\t** FAILED: %s" % e.response["error"])

        with open(JSON_PATH, "w+") as f:
            f.write(DUMP(read))
        with open(JSON_PATH, "r+") as f:
            read = LOAD(f)

        print("Resting 1 hour...")

        time.sleep(3600)


except Exception as e:
    print(type(e))
    print(dir(e))

except KeyboardInterrupt:
    print("Pressed CTRL+C...")

finally:
    print("Exiting...")
    p.flush(100)
