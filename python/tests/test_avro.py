import datetime
import os
import ssl
from io import BytesIO
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup
from fastavro import (
    parse_schema,
    reader,
    schemaless_reader,
    schemaless_writer,
    writer,
)


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

# 'records' can be an iterable (including generator)
records = [
    {
        "id": 10005,
        "title": "대학교 공지 1",
        "date": "2020-12-01",
        "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=10005",
        "writer": "CSW",
    },
    {
        "id": 10006,
        "title": "대학교 공지 2",
        "date": "2020-12-02",
        "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=10006",
        "writer": "CSW",
    },
    {
        "id": 10007,
        "title": "대학교 공지 3",
        "date": "2020-12-04",
        "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=10007",
        "writer": "CSW",
    },
    {
        "id": 10008,
        "title": "대학교 공지 4",
        "date": "2020-12-04",
        "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=10008",
        "writer": "CSW",
    },
    {
        "id": 10009,
        "title": "대학교 공지 5",
        "date": "2020-12-11",
        "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=10009",
        "writer": "CSW",
    },
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AVRO_PATH = os.path.join(BASE_DIR, "ajou.avro")

ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
LENGTH = 10


def makeData(postId, postTitle, postDate, postLink, postWriter):
    return {
        "id": postId,
        "title": postTitle,
        "date": postDate,
        "link": postLink,
        "writer": postWriter,
    }


def parser():
    context = ssl._create_unverified_context()
    try:
        result = urlopen(
            f"{ADDRESS}?mode=list&&articleLimit={LENGTH}&article.offset=0",
            context=context,
        )
    except HTTPError:
        print("Seems like the server is down now.")
        return None, None, None, None
    html = result.read()
    soup = BeautifulSoup(html, "html.parser")
    ids = soup.select("table > tbody > tr > td.b-num-box")
    posts = soup.select("table > tbody > tr > td.b-td-left > div > a")
    dates = soup.select("table > tbody > tr > td.b-td-left > div > div > span.b-date")
    writers = soup.select(
        "table > tbody > tr > td.b-td-left > div > div.b-m-con > span.b-writer"
    )
    return ids, posts, dates, writers


def test_single_record():
    # To send with producer
    message = {
        "id": 10000,
        "title": "[FastAVRO] 테스트 공지 제목",
        "date": "20.12.23",
        "link": "https://somelink",
        "writer": "alfex4936",
    }

    # How producer produces single data
    producer_rb = BytesIO()
    schemaless_writer(producer_rb, parsed_schema, message)  # write one record
    produced_data = producer_rb.getvalue()

    # How consumer reads single record
    consumer_rb = BytesIO(produced_data)
    decoded = schemaless_reader(consumer_rb, parsed_schema)  # read one record
    assert decoded == {
        "id": 10000,
        "title": "[FastAVRO] 테스트 공지 제목",
        "date": "20.12.23",
        "link": "https://somelink",
        "writer": "alfex4936",
    }

    # {'id': 10000, 'title': '[FastAVRO] 테스트 공지 제목', 'date': '20.12.23', 'link': 'https://somelink', 'writer': 'alfex4936'}


def test_write():
    # Writing
    with open(AVRO_PATH, "wb") as out:
        writer(out, parsed_schema, records)

    assert os.path.isfile(AVRO_PATH), "Writing avro file gone wrong."


def test_read():
    # Reading
    with open(AVRO_PATH, "rb") as fo:
        for record in reader(fo):
            print(record)
            assert isinstance(record["id"], int)


def test_consumer():
    msg = [
        {
            "id": 10000,
            "title": "[\ud559\uc2b5\ubc95] \uc131\uacf5\ud558\ub294 \ud559\uc2b5\ub9ac\ub354\ub97c \uc704\ud55c \ud559\uc2b5\ub9ac\ub354\uc6cc\ud06c\uc20d \uc548\ub0b4",
            "date": "20.12.07",
            "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=104863&article.offset=0&articleLimit=10",
            "writer": "\uad50\uc218\ud559\uc2b5\uac1c\ubc1c\uc13c\ud130",
        },
        {
            "id": 10001,
            "title": "[\ud559\uc2b5\ubc95] \uc131\uacf5\ud558\ud558 \ud559\uc2b5\ub9ac\ub354\ub97c \uc704\ud55c \ud559\uc2b5\ub9ac\ub354\uc6cc\ud06c\uc20d \uc548\ub0b4",
            "date": "20.12.08",
            "link": "https://www.ajou.ac.kr/kr/ajou/notice.do?mode=view&articleNo=104863&article.offset=0&articleLimit=10",
            "writer": "\uad50\uc218\ud559\uc2b5\uac1c\ubc1c\uc13c\ud130",
        },
    ]
    newFile = BytesIO()
    writer(newFile, parsed_schema, msg)
    newFile.seek(0)
    for record in reader(newFile):
        print(record)


def test_parse():
    processedIds = []  # already parsed

    with open(AVRO_PATH, "rb") as fo:
        for record in reader(fo):
            processedIds.append(record["id"])
    print("ALREADY PARSED:", processedIds)

    records = []
    print()  # Section
    PRODUCED = 0  # How many messages did it send

    # No checking on last parsed date, always starts new
    print("Trying to parse new posts...")
    ids, posts, dates, writers = parser()  # 다시 파싱
    assert ids is not None, f"Check your parser: {ids}."

    for i in range(LENGTH):
        postId = int(ids[i].text.strip())
        if postId in processedIds:  # Already sent
            continue
        else:
            processedIds.append(postId)
        postLink = ADDRESS + posts[i].get("href")
        postTitle = posts[i].text.strip()
        postDate = dates[i].text.strip()
        postWriter = writers[i].text

        # Removing a name duplication
        duplicate = "[" + postWriter + "]"
        if duplicate in postTitle:  # writer: [writer] title
            postTitle = postTitle.replace(duplicate, "").strip()  # -> writer: title

        data = makeData(postId, postTitle, postDate, postLink, postWriter)
        records.append(data)

        print("\n>>> Sending a new post...:", postId)
        PRODUCED += 1

        # Producer 자리

    if PRODUCED:
        print(f"Sent {PRODUCED} post(s)...")
        with open(AVRO_PATH, "wb") as out:
            writer(out, parsed_schema, records)
    else:
        print("\t** No new posts yet")
    print("Parsed at", datetime.datetime.now())

