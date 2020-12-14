import mysql.connector

import requests
from bs4 import BeautifulSoup

from contextlib import contextmanager
import os

# mysql.connector.paramstyle = "pyformat"

ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
LENGTH = 10
FILTER_WORDS = ()  # only parse if notices contain these words
INSERT_COMMAND = (
    "INSERT INTO notices (id, title, date, link, writer) " "VALUES (%s, %s, %s, %s, %s)"
)
DUPLICATE_COMMAND = "SELECT EXISTS(SELECT * FROM notices WHERE id = %(id)s)"
UPDATE_COMMAND = "UPDATE notices SET date = %(date)s WHERE id = 1"


@contextmanager
def OPEN_DB():
    # connect to my local MySQL instance using connection string
    db = mysql.connector.connect(
        host="localhost",
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database="ajou_notices",
        charset="utf8",
    )
    cursor = db.cursor(buffered=True)
    yield cursor
    db.commit()  # always commits
    cursor.close()
    db.close()


# Update MySQL database
def updateDB(cursor):
    ids, posts, dates, writers = parser()  # 다시 파싱
    for i in range(LENGTH):
        postId = int(ids[i].text.strip())

        cursor.execute(DUPLICATE_COMMAND, {"id": postId})
        if cursor.fetchone()[0]:  # (1, )
            continue  # postId exists

        postLink = posts[i].get("href")
        postTitle = posts[i].text.strip()
        postDate = dates[i].text.strip()
        postWriter = writers[i].text

        duplicate = "[" + postWriter + "]"
        if duplicate in postTitle:  # writer: [writer] title
            postTitle = postTitle.replace(duplicate, "").strip()  # -> writer: title

        data = (postId, postTitle, postDate, ADDRESS + postLink, postWriter)

        cursor.execute(INSERT_COMMAND, data)


# Ajou notices parser
def parser():
    req = requests.get(f"{ADDRESS}?mode=list&&articleLimit={LENGTH}&article.offset=0")
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


def test_open():
    with OPEN_DB() as cursor:
        assert cursor is not None, "contextmanager doesn't work."


def test_dbconnect():
    db = mysql.connector.connect(
        host="localhost",
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database="ajou_notices",
        charset="utf8",
    )
    assert db is not None, "db connection is wrong."
    db.close()


def test_duplicate():
    with OPEN_DB() as cursor:
        cursor.execute(DUPLICATE_COMMAND, {"id": 54321})  # 54321 is for the test
        if cursor.fetchone()[0]:  # (1, )
            print("exists")


def test_updateDB():
    with OPEN_DB() as cursor:
        updateDB(cursor)

        cursor.execute(
            "SELECT * FROM notices ORDER BY id"
        )  # query = SELECT * FROM testdb.notices LIMIT 3;

        for row in cursor:
            print(row)
            assert row is not None


def test_lastparsed():
    """Test LAST_PARSED update"""
    from datetime import datetime

    now = datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    with OPEN_DB() as cursor:
        cursor.execute(UPDATE_COMMAND, {"date": now})

        cursor.execute("SELECT date FROM notices WHERE id = 1")

        fetch = cursor.fetchone()[0]
        assert fetch == now, "Updating LAST_PARSED date failed."
        print(fetch)  # 2020-12-14 20:29:24.222095


def test_delete():
    with OPEN_DB() as cursor:
        cursor.execute("DELETE FROM notices WHERE id = 12245")


if __name__ == "__main__":
    # INSERT test data
    data = (54321, "내 공지", "20.12.14", ADDRESS + "/csw", "csw")
    with OPEN_DB() as cursor:
        cursor.execute(INSERT_COMMAND, data)
