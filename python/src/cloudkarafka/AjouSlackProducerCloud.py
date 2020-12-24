import datetime
import json
import os
import ssl
import time
from urllib.error import HTTPError
from urllib.request import urlopen

import mysql.connector
from bs4 import BeautifulSoup
from confluent_kafka import Producer


class AjouParser:
    """
    Ajou notices Parser using Slack API and Apache Kafka (MySQL)
    
    Methods
    -------
    run(server=os.environ["CLOUDKARAFKA_BROKER"], database="ajou_notices")
    
    Usage
    -----
        ajou = AjouParser(Kafka_server_ip, mysql_db_name)
        ajou.run()
    """

    ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
    LENGTH = 10

    # MySQL commands
    INSERT_COMMAND = (
        "INSERT INTO notices (id, title, date, link, writer) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    DUPLICATE_COMMAND = "SELECT EXISTS(SELECT * FROM notices WHERE id = %(id)s)"
    UPDATE_COMMAND = "UPDATE notices SET date = %(date)s WHERE id = 1"

    __slots__ = ("db", "cursor", "settings", "producer", "topic")

    def __init__(
        self, server=os.environ["CLOUDKARAFKA_BROKERS"], database="ajou_notices"
    ):
        print("Initializing...")

        # MySQL
        self.db = mysql.connector.connect(
            host="localhost",
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
            database=database,
            charset="utf8",
        )
        self.cursor = self.db.cursor(buffered=True)

        # Kafka
        self.topic = os.environ["CLOUDKARAFKA_TOPIC"]

        self.settings = {  # Producer settings
            "bootstrap.servers": server,
            "compression.type": "snappy",  # High throughput
            # From CloudKarafka
            "session.timeout.ms": 6000,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-256",
            "sasl.username": os.environ["CLOUDKARAFKA_USERNAME"],
            "sasl.password": os.environ["CLOUDKARAFKA_PASSWORD"],
        }
        self.producer = Producer(self.settings)

    def run(self, period=3600):  # period (second)
        """Check notices from html per period and sends data to Kafka Consumer."""
        p = self.producer
        db = self.db
        cursor = self.cursor

        try:
            while True:  # 1시간마다 무한 반복
                print()  # Section
                PRODUCED = 0  # How many messages did it send?

                cursor.execute("SELECT date FROM notices WHERE id = 1")
                LAST_PARSED = datetime.datetime.strptime(
                    cursor.fetchone()[0], "%Y-%m-%d %H:%M:%S.%f"
                )  # db load date where id = 1

                now = self.getTimeNow()
                diff = (now - LAST_PARSED).seconds

                print("Last parsed at", LAST_PARSED)
                if (diff / period) < 1:  # 업데이트 후 period시간이 안 지났음, 대기
                    print(f"Wait for {period - diff} seconds to sync new posts.")
                    time.sleep(period - diff)

                print("Trying to parse new posts...")
                ids, posts, dates, writers = self.parser()  # 다시 파싱
                assert ids is not None, f"Check your parser: {ids}."

                # 파싱 오류가 없으면 업데이트
                cursor.execute(
                    self.UPDATE_COMMAND, {"date": now.strftime("%Y-%m-%d %H:%M:%S.%f")},
                )

                for i in range(self.LENGTH):
                    postId = ids[i].text.strip()

                    cursor.execute(
                        self.DUPLICATE_COMMAND, {"id": int(postId)}
                    )  # db duplication check
                    if cursor.fetchone()[0]:  # (1, )
                        continue  # postId exists

                    postLink = self.ADDRESS + posts[i].get("href")
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
                        postLink,
                        postWriter,
                    )
                    kafkaData = self.makeData(
                        postId, postTitle, postDate, postLink, postWriter
                    )

                    cursor.execute(self.INSERT_COMMAND, dbData)  # db insert

                    print("\n>>> Sending a new post...:", postId)

                    try:
                        p.produce(
                            self.topic,
                            value=json.dumps(kafkaData[postId]),
                            callback=self.acked,
                        )
                    except BufferError as e:
                        print(
                            f"Local producer queue is full. ({len(p)} messages awaiting delivery)"
                        )
                    PRODUCED += 1
                    p.poll(0)  # 데이터 Kafka에게 전송, second

                if PRODUCED:
                    print(f"Sent {PRODUCED} post(s)...")
                    p.flush()
                else:
                    print("\t** No new posts yet")
                print("Parsed at", now)

                db.commit()  # save data
                print(f"Resting {period // 3600} hour...")
                time.sleep(period)

        except Exception as e:  # General exceptions
            print(e)
            print(dir(e))
        except KeyboardInterrupt:
            print("Pressed CTRL+C...")
        finally:
            print("\nExiting...")
            cursor.close()
            db.commit()
            db.close()
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
    ajou = AjouParser()
    ajou.run()
