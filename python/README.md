<div align="center">
<p>
    <img width="480" src="https://www.andplus.com/hs-fs/hubfs/kafkalogo.jpg?&name=kafkalogo.jpg">
</p>
<h1>Apache Kafka 공부 in Python</h1>
    <h5>v2.13-2.6.0</h5>

[Apache Kafka](https://kafka.apache.org/)

</div>

## 설치
[Kafka 다운로드](https://kafka.apache.org/downloads)

*Make sure to Download "Binary"*

## 실행

zookeeper & kafka 서버 실행

```console

WIN10@DESKTOP:~$ zookeeper-server-start config/zookeeper.properties

WIN10@DESKTOP:~$ kafka-server-start config/server.properties

```

Kafka 설치

```console

WIN10@DESKTOP:~$ pip install confluent-kafka

```

## Slack API with Ajou University notices parser

[Try Ajou notice parser here](https://kafkatesthq.slack.com/archives/C01G2CR5MEE)

[Get Slack API here](https://api.slack.com/)

First, invite your bot to your channel. (In this example, we set it as "#아주대")

The [producer](https://github.com/Alfex4936/kafka-Studies/tree/main/python/src/AjouSlackProducer.py) will notify the [consumer](https://github.com/Alfex4936/kafka-Studies/tree/main/python/src/AjouSlackConsumer.py) whenever there are new notices.

The *producer* checks new notices per an hour, saves latest 10 notices to json file,
and sees if there is/are a new notice/s.

If there is a new notice, it sends {"TITLE": "title", "DATE": "post date", "LINK": "http address", "WRITER": "writer"} to the consumer.

The *consumer* checks new datas every 5 seconds, if it gets a new data,
it consumes the data and leave a comment like this below image.

<div align="center">
<p>
    <img width="480" src="https://github.com/Alfex4936/kafka-Studies/blob/main/img/slack_ajou.png">
</p>
</div>

:b: Run the server first to see the results.

```console
WIN10@DESKTOP:~$ python AjouSlackProducer.py

WIN10@DESKTOP:~$ python AjouSlackConsumer.py
```

## Slack API Producer Usage

[Get Slack API](https://api.slack.com/)

Invite your bot to user community channel and your developer channel.

This producer sends a data if users post comments containing a word, "bug".

```json
example data)
{ "USER": "ikr", "TEXT": "I found a bug! I can keep copying my items to my inventory." }
```

Then the consumer below, will consume the data and posts a message in your developer channel.

"USERNAME: MESSAGE: Please see if we can fix it right here, right now"


## Slack API Consumer Usage

Modified version of [official Confluent example](https://github.com/confluentinc/infoq-kafka-ksql)

[Get Slack API](https://api.slack.com/)

Add "chat:write" scope to both user and bot.

Copy Bot User OAuth Access Token from OAuth & Permissions section.

```console
/INVITE @BOTNAME
```

to your channel if you see an error, "** FAILED: not_in_channel"

Using CLI or producers, send a data to your kafka topic.

```console
kafka-console-producer --broker-list localhost:9092 --topic SLACK-KAFKA
> {"CLUB_STATUS":"platinum","EMAIL":"ikr@kakao.com","STARS":1,"MESSAGE":"Exceeded all my expectations!"}
```

Whenever you send a data to kafka,
this consumer consumes email and message from the user,
and posts "EMAIL just left a bad review" to your slack channel.

```console
Result

Sending message "`ikr@kakao.com` just left a bad review :disappointed:
> Exceeded all my expectations!

_Please contact them immediately and see if we can fix the issue *right here, right now*_" to channel kafka
```

<div align="center">
<p>
    <img width="480" src="https://github.com/Alfex4936/kafka-Studies/blob/main/img/slack.png">
</p>
</div>

## FastAvro Producer / Consumer

Install fastavro with pip

```console
WIN10@DESKTOP:~$ pip install fastavro
```

Example Schema to use
```python
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
```

How to produce single data with Producer &
consume it with Consumer at [pytest](https://github.com/Alfex4936/kafka-Studies/blob/main/python/tests/test_avro.py#L110)

