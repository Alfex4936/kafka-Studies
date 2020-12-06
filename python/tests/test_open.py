import json
import os
from contextlib import contextmanager
from pathlib import Path


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PATH = os.path.join(BASE_DIR, "test.json")


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

