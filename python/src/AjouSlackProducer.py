import datetime
import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup
from config import Config
from confluent_kafka import Producer


DUMP = lambda x: json.dumps(x)
LOAD = lambda x: json.load(x)


class AjouParserJSON:
    """
    Ajou notices Parser using Slack API and Apache Kafka (JSON)
    
    JSON file will be saved in your current directory.
    
    Methods
    -------
    run(server=Config.VM_SERVER, json_name="already_read.json")
    
    Usage
    -----
        ajou = AjouParserJSON(Kafka_server_ip, json_path)
        ajou.run()
    """

    # HTML
    ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
    LENGTH = 10

    # JSON
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    MAXIMUM_DAY = 7  # remove notices in json that were posted more than 7days ago
    global DUMP, LOAD

    __slots__ = ("settings", "producer", "read")

    def __init__(self, server=Config.VM_SERVER, json_name="already_read.json"):
        print("Initializing...")

        self.JSON_PATH = os.path.join(self.BASE_DIR, json_name)

        self.settings = {  # Producer settings
            "bootstrap.servers": server,
            "enable.idempotence": True,  # Safe
            "acks": "all",  # Safe
            "retries": 5,  # Safe
            "max.in.flight": 5,  # High throughput
            "compression.type": "lz4",  # High throughput
            "linger.ms": 5,  # High throughput
        }
        self.producer = Producer(self.settings)

        # 공지 Parser
        if not Path(self.JSON_PATH).is_file():  # 파일 없으면 기본 형식 만듬
            base_data = {"POSTS": {}, "LAST_PARSED": "1972-12-01 07:00:00.000000"}

            with open(self.JSON_PATH, "a+") as f:
                f.write(DUMP(base_data))

        self.read = None

        # json read
        with open(self.JSON_PATH, "r+") as f_read:
            self.read = LOAD(f_read)

        self.read = self.checkOldness(self.read)

    def run(self, period=3600):  # period (second)
        """Check notices from html per period and sends data to Kafka Consumer."""
        p = self.producer
        read = self.read

        try:
            while True:  # 1시간마다 무한 반복
                print()  # Section
                PRODUCED = 0  # How many messages did it send?

                LAST_PARSED = datetime.datetime.strptime(
                    read["LAST_PARSED"], "%Y-%m-%d %H:%M:%S.%f"
                )

                now = self.getTimeNow()
                diff = (now - LAST_PARSED).seconds

                print("Last parsed at", LAST_PARSED)
                if (diff / period) < 1:  # 업데이트 후 period시간이 안 지났음, 대기
                    print(f"Wait for {period - diff} seconds to sync new posts.")
                    time.sleep(period - diff)

                print("Trying to parse new posts...")
                ids, posts, dates, writers = self.parser()  # 다시 파싱
                assert ids is not None, f"Check your parser: {ids}."

                for i in range(self.LENGTH):
                    postId = ids[i].text.strip()
                    if postId in read["POSTS"]:
                        continue
                    postLink = self.ADDRESS + posts[i].get("href")
                    postTitle = posts[i].text.strip()
                    postDate = dates[i].text.strip()
                    postWriter = writers[i].text

                    # Removing a name duplication
                    duplicate = "[" + postWriter + "]"
                    if duplicate in postTitle:  # writer: [writer] title
                        postTitle = postTitle.replace(
                            duplicate, ""
                        ).strip()  # -> writer: title

                    kafkaData = self.makeData(
                        postId, postTitle, postDate, postLink, postWriter
                    )

                    print("\n>>> Sending a new post...:", postId)
                    PRODUCED += 1

                    p.produce(
                        Config.AJOU_TOPIC_ID,
                        value=DUMP(kafkaData[postId]),
                        callback=self.acked,
                    )
                    p.poll(1)  # 데이터 Kafka에게 전송, second

                    read["LAST_PARSED"] = now.strftime("%Y-%m-%d %H:%M:%S.%f")
                    read["POSTS"].update(kafkaData)

                if PRODUCED:
                    print(f"Sent {PRODUCED} post(s)...")
                else:
                    print("\t** No new posts yet")
                print("Parsed at", now)

                with open(self.JSON_PATH, "w+") as f:
                    f.write(DUMP(read))
                with open(self.JSON_PATH, "r+") as f:
                    read = LOAD(f)

                print(f"Resting {period // 3600} hour...")
                time.sleep(period)

        except Exception as e:  # General exceptions
            print(dir(e))
        except KeyboardInterrupt:
            print("Pressed CTRL+C...")
        finally:
            print("\nExiting...")
            p.flush(100)

    # Producer callback function
    @staticmethod
    def acked(err, msg):
        if err is not None:
            print(
                "\t** Failed to deliver message: {0}: {1}".format(
                    msg.value(), err.str()
                )
            )
        else:
            print("Message produced correctly...")

    @staticmethod
    def makeData(postId, postTitle, postDate, postLink, postWriter):
        return {
            postId: {
                "TITLE": postTitle,
                "DATE": postDate,
                "LINK": postLink,
                "WRITER": postWriter,
            }
        }

    def checkOldness(self, jsonFile):
        today = datetime.datetime.today()
        today = datetime.datetime(today.year, today.month, today.day)
        for post in list(jsonFile["POSTS"]):
            currentDate = jsonFile["POSTS"][post]["DATE"]  # string
            savedDate = datetime.datetime.strptime(currentDate, "%y.%m.%d")
            if (today - savedDate).days > self.MAXIMUM_DAY:
                del jsonFile["POSTS"][post]

        with open(self.JSON_PATH, "w+") as f:
            f.write(json.dumps(jsonFile))

        with open(self.JSON_PATH, "r+") as f:
            read = json.load(f)

        return read

    @staticmethod
    def getTimeNow() -> datetime.datetime:
        return datetime.datetime.now()

    # Ajou notices parser
    def parser(self):
        context = ssl._create_unverified_context()
        try:
            result = urlopen(
                f"{self.ADDRESS}?mode=list&&articleLimit={self.LENGTH}&article.offset=0",
                context=context,
            )
        except HTTPError:
            print("Seems like the server is down now.")
            return None, None, None, None
        html = result.read()
        soup = BeautifulSoup(html, "html.parser")
        ids = soup.select("table > tbody > tr > td.b-num-box")
        posts = soup.select("table > tbody > tr > td.b-td-left > div > a")
        dates = soup.select(
            "table > tbody > tr > td.b-td-left > div > div > span.b-date"
        )
        writers = soup.select(
            "table > tbody > tr > td.b-td-left > div > div.b-m-con > span.b-writer"
        )
        return ids, posts, dates, writers


if __name__ == "__main__":
    ajou = AjouParserJSON()
    ajou.run()
