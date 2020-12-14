import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "test.json")
MAXIMUM_DAY = 6


def checkOldness(jsonFile):
    today = datetime.today()
    today = datetime(today.year, today.month, today.day)
    for post in list(jsonFile["POSTS"]):
        currentDate = jsonFile["POSTS"][post]["DATE"]  # string
        savedDate = datetime.strptime(currentDate, "%y.%m.%d")
        if (today - savedDate).days > MAXIMUM_DAY:
            print(f"removing {post}...")
            del jsonFile["POSTS"][post]

    with open(JSON_PATH, "w+") as f:
        f.write(json.dumps(jsonFile))

    with open(JSON_PATH, "r+") as f:
        read = json.load(f)

    return read


@contextmanager
def jsonify(mode):
    f = open(JSON_PATH, mode=mode)
    read = json.load(f)
    yield read
    f.close()


def test_open():
    if not Path(JSON_PATH).is_file():
        base_data = {"POSTS": {}}
        with open(JSON_PATH, "a+") as f_read:
            f_read.write(json.dumps(base_data))

    with jsonify("r+") as f:
        data = f

    assert data is not None, "data is None."


def test_remove():
    with open(JSON_PATH, "r+") as f_read:
        read = json.load(f_read)
    read = checkOldness(read)

    today = datetime.today()
    today = datetime(today.year, today.month, today.day)
    old = today - timedelta(days=MAXIMUM_DAY)

    for post in list(read["POSTS"]):
        currentDate = read["POSTS"][post]["DATE"]  # string
        savedDate = datetime.strptime(currentDate, "%y.%m.%d")
        assert savedDate > old, f"{MAXIMUM_DAY}일이 지난 공지는 제거되어야 함."


def test_length():
    with open(JSON_PATH, "r+") as f_read:
        read = json.load(f_read)

    lengths = 0
    seen = 0

    for post in read["POSTS"]:
        seen += 1
        lengths += len(read["POSTS"][post]["TITLE"])

    print(lengths / seen)
