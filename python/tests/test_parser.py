import json
import ssl

from urllib.request import urlopen
from bs4 import BeautifulSoup


ADDRESS = "https://www.ajou.ac.kr/kr/ajou/notice.do"
LENGTH = 10
FILTER_WORDS = ("설문", "기프트", "납부", "등록금")  # only parse if notices contain these words

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


def parser():
    # req = requests.get(f"{ADDRESS}?mode=list&&articleLimit={LENGTH}&article.offset=0")
    context = ssl._create_unverified_context()
    result = urlopen(
        f"{ADDRESS}?mode=list&srCategoryId=3&srSearchKey=&srSearchVal=&articleLimit={LENGTH}&article.offset=0",
        context=context,
    )

    html = result.read()
    soup = BeautifulSoup(html, "html.parser")
    ids = soup.select("table > tbody > tr > td.b-num-box")
    posts = soup.select("table > tbody > tr > td.b-td-left > div > a")
    dates = soup.select("table > tbody > tr > td.b-td-left > div > div > span.b-date")
    writers = soup.select(
        "table > tbody > tr > td.b-td-left > div > div.b-m-con > span.b-writer"
    )
    return ids, posts, dates, writers


# Test #1
def test_parse():
    ids, posts, dates, writers = parser()
    assert len(ids) == 10, f"Check your parser: {ids}"
    assert len(posts) == 10, f"Check your parser: {posts}"
    assert len(dates) == 10, f"Check your parser: {dates}"
    assert len(writers) == 10, f"Check your parser: {writers}"
    for i in range(LENGTH):
        postTitle = posts[i].text.strip()
        # if FILTER_WORDS:
        #     FILTERD = False
        #     for filter in FILTER_WORDS:
        #         if filter in postTitle:
        #             FILTERD = True
        #             break
        #     if not FILTERD:
        #         continue

        postId = ids[i].text.strip()
        postLink = posts[i].get("href")
        postDate = dates[i].text.strip()
        postWriter = writers[i].text
        # assert int(postId) > 10000, f"postId is None."
        assert postLink is not None, f"postLink is None."
        assert postTitle is not None, f"postTitle is None."
        assert postDate is not None, f"postDate is None."
        assert postWriter is not None, f"postWriter is None."

        data = makeJson(postId, postTitle, postDate, postLink, postWriter)
        temp = json.dumps(data[postId])
        print("data", json.loads(temp))


if __name__ == "__main__":
    test_parse()
    # print(next(iter(read["POSTS"].keys())))  # Last Key
