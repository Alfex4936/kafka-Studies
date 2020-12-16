import datetime
import json
import os
import time

import mysql.connector
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
        print("Message produced: {0}...".format(msg.value()))


def checkOldness(db):
    pass


def makeData(postId, postTitle, postDate, postLink, postWriter):
    return {
        postId: {
            "TITLE": postTitle,
            "DATE": postDate,
            "LINK": postLink,
            "WRITER": postWriter,
        }
    }


# Ajou notices parser
def parser():
    try:
        req = requests.get(f"{ADDRESS}?mode=list&&articleLimit=10&article.offset=0")
        req.raise_for_status()
    except requests.exceptions.ConnectionError:
        print("Seems like the server is down now.")
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
LENGTH = 10
MAXIMUM_DAY = 7  # remove notices in json that were posted more than 7days ago
DUMP = lambda x: json.dumps(x)

# MySQL commands
INSERT_COMMAND = (
    "INSERT INTO notices (id, title, date, link, writer) " "VALUES (%s, %s, %s, %s, %s)"
)
DUPLICATE_COMMAND = "SELECT EXISTS(SELECT * FROM notices WHERE id = %(id)s)"
UPDATE_COMMAND = "UPDATE notices SET date = %(date)s WHERE id = 1"

# MySQL
db = mysql.connector.connect(
    host="localhost",
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"],
    database="ajou_notices",
    charset="utf8",
)
cursor = db.cursor(buffered=True)

# Bot User OAuth Access Token
# used scopes: channels:history, channels:read, chat:write, im:history, mpim:history, users:read
token = os.environ["SLACK_BOT_TOKEN"]

# Getting LAST_PARSED from MySQL, where id = 1
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M:%S.%f")

cursor.execute("SELECT date FROM notices WHERE id = 1")

# Set last parsed time to rest 1 hour well
LAST_PARSED = cursor.fetchone()[0]
assert LAST_PARSED is not None, "LAST_PASRED is None."

# init parser
ids, posts, dates, writers = parser()

# Slack API 초기화
sc = WebClient(token)
channel = "C01G2CR5MEE"  # 아주대

# Kafka Producer 만들기  "localhost:9092"
settings = {
    "bootstrap.servers": Config.VM_SERVER,
    # Safe Producer settings
    "enable.idempotence": True,
    "acks": "all",
    "retries": 10000000,
    "max.in.flight": 5,
    "compression.type": "lz4",
    "linger.ms": 5,
}  # "enable.idempotence": True, "retries": 5
p = Producer(settings)

try:
    while True:  # 1시간마다 무한 반복
        PRODUCED = 0

        cursor.execute("SELECT date FROM notices WHERE id = 1")
        LAST_PARSED = datetime.datetime.strptime(
            cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S.%f"
        )  # db load date where id = 1
        assert LAST_PARSED is not None, "LAST_PASRED is None."
        try:
            now = datetime.datetime.now()
            diff = (now - LAST_PARSED).seconds

            print("Last parsing:", LAST_PARSED)
            if diff / 3600 < 1:  # 업데이트 후 1시간이 안 지났음, 대기
                print(f"Wait for {3600 - diff} seconds to sync new posts.")
                time.sleep(3600 - diff)

            print("Trying to parse new posts...")
            ids, posts, dates, writers = parser()  # 다시 파싱

            # 파싱 오류가 없으면 업데이트
            cursor.execute(
                UPDATE_COMMAND, {"date": now.strftime("%Y-%m-%d %H:%M:%S.%f")}
            )

            for i in range(LENGTH):
                postId = ids[i].text.strip()

                cursor.execute(
                    DUPLICATE_COMMAND, {"id": int(postId)}
                )  # db duplication check
                if cursor.fetchone()[0]:  # (1, )
                    continue  # postId exists

                postLink = posts[i].get("href")
                postTitle = posts[i].text.strip()
                postDate = dates[i].text.strip()
                postWriter = writers[i].text

                duplicate = "[" + postWriter + "]"
                if duplicate in postTitle:  # writer: [writer] title
                    postTitle = postTitle.replace(
                        duplicate, ""
                    ).strip()  # -> writer: title

                dbData = (
                    int(postId),
                    postTitle,
                    postDate,
                    ADDRESS + postLink,
                    postWriter,
                )
                kafkaData = makeData(
                    postId, postTitle, postDate, ADDRESS + postLink, postWriter
                )

                cursor.execute(INSERT_COMMAND, dbData)  # db insert

                print("Sending a new post...:", postId)
                PRODUCED += 1
                p.produce(
                    Config.AJOU_TOPIC_ID, value=DUMP(kafkaData[postId]), callback=acked,
                )
                p.poll(1)  # 데이터 Kafka에게 전송

            if PRODUCED:
                print(f"Sent {PRODUCED} posts...")
            else:
                print("No new posts yet...")
            print("Parsed:", now)

        except SlackApiError as e:
            assert e.response["ok"] is False
            print("\t** FAILED: %s" % e.response["error"])

        db.commit()  # save data
        print("Resting 1 hour...")
        time.sleep(3600)


except Exception as e:
    print(type(e))
    print(dir(e))

except KeyboardInterrupt:
    print("Pressed CTRL+C...")

finally:
    print("Exiting...")
    db.commit()
    cursor.close()
    db.close()
    p.flush(100)
