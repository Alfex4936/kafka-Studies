import datetime
import os
import ssl
import time
from io import BytesIO
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup
from confluent_kafka import Producer
from fastavro import parse_schema, reader, schemaless_writer, writer


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


class AjouParserAVRO:
    """
    Ajou notices Parser using Slack API and Apache Kafka (AVRO)
    
    AVRO file will be saved in your current directory.
    
    Methods
    -------
    run(server=Config.VM_SERVER, avro_name="ajou.avro")
    
    Usage
    -----
        ajou = AjouParserAVRO(Kafka_server_ip, avro_name)
        ajou.run()
    """

    # HTML
    ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
    LENGTH = 10

    # AVRO
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    __slots__ = ("settings", "producer", "AVRO_PATH")

    def __init__(self, server=os.environ["VM_SERVER"], avro_name="ajou.avro"):
        print("Initializing...")

        self.AVRO_PATH = os.path.join(
            self.BASE_DIR, avro_name
        )  # saves into current dir

        self.settings = {  # Producer settings
            "bootstrap.servers": server,
            "enable.idempotence": True,  # Safe
            "acks": "all",  # Safe
            "retries": 5,  # Safe
            "max.in.flight": 5,  # High throughput
            "compression.type": "snappy",  # High throughput
            "linger.ms": 5,  # High throughput
        }
        self.producer = Producer(self.settings)

    def run(self, period=3600):  # period (second)
        """Check notices from html per period and sends data to Kafka Consumer."""
        p = self.producer
        processedIds = []  # already parsed

        with open(self.AVRO_PATH, "rb") as fo:
            for record in reader(fo):
                processedIds.append(record["id"])  # add id to already parsed list

        try:
            while True:  # 1시간마다 무한 반복
                records = []
                print()  # Section
                PRODUCED = 0  # How many messages did it send

                # No checking on last parsed date, always starts new
                print("Trying to parse new posts...")
                ids, posts, dates, writers = self.parser()  # 다시 파싱
                assert ids is not None, f"Check your parser: {ids}."

                for i in range(self.LENGTH):  # check all 10 notices
                    postId = int(ids[i].text.strip())
                    if postId in processedIds:
                        continue
                    else:
                        processedIds.append(postId)
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

                    data = self.makeData(
                        postId, postTitle, postDate, postLink, postWriter
                    )
                    rb = BytesIO()
                    schemaless_writer(rb, parsed_schema, data)  # write one record

                    records.append(data)  # to write avro

                    print("\n>>> Sending a new post...:", postId)
                    PRODUCED += 1
                    p.produce(
                        "AJOU-NOTIFY", value=rb.getvalue(), callback=self.acked,
                    )
                    p.poll(1)  # 데이터 Kafka에게 전송, second

                if PRODUCED:
                    print(f"Sent {PRODUCED} post(s)...")

                    with open(self.AVRO_PATH, "wb") as out:
                        # write avro only when there are updates
                        writer(out, parsed_schema, records)
                else:
                    print("\t** No new posts yet")
                print("Parsed at", self.getTimeNow())

                print(f"Resting {period // 3600} hour...")
                time.sleep(period)

        except Exception as e:  # General exceptions
            print(e)
            print(dir(e))
        except KeyboardInterrupt:
            print("Pressed CTRL+C...")
        finally:
            print("\nExiting...")
            p.flush(100)

    @staticmethod
    def error_cb(error):
        print(">>>", error)

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
            "id": postId,
            "title": postTitle,
            "date": postDate,
            "link": postLink,
            "writer": postWriter,
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
    ajou = AjouParserAVRO()
    ajou.run()
