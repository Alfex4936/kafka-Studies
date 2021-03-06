<div align="center">
<p>
    <img width="480" src="https://www.andplus.com/hs-fs/hubfs/kafkalogo.jpg?&name=kafkalogo.jpg">
</p>
<h1>Apache Kafka 공부 (Java, Python, Go)</h1>
    <h5>v2.13-2.6.0</h5>
    
<a href="https://hits.seeyoufarm.com"><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2FAlfex4936%2Fkafka-Studies&count_bg=%233DA1C8&title_bg=%23555555&icon=apachekafka.svg&icon_color=%23E7E7E7&title=%2B&edge_flat=false"/></a>

[Apache Kafka](https://kafka.apache.org/)

</div>

## 목차

<table>
    <tr><td width=40% valign=top>

* Kafka (Python)
    * [Simple Producer](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/ProducerDemo.py)
    * [Producer with callback](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/ProducerDemoCallBack.py)
    * [Producer with Slack API](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/SlackKafkaProducer.py)
    * [Consumer with Slack API](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/SlackKafkaConsumer.py)
    * [Ajou notices Parsing Producer with Slack API](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/AjouSlackProducer.py)
    * [Ajou notices Parsing Producer with Slack API using MySQL](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/AjouSlackProducerMySQL.py)
    * [Ajou notices Parsing Consumer with Slack API](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/AjouSlackConsumer.py)
    * [FastAvro Producer example](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/avro/AjouSlackProducerAvro.py)
    * [FastAvro Consumer example](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/avro/AjouSlackConsumerAvro.py)
    * [Cloud Kafka Producer example](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/cloudkarafka/AjouSlackProducerCloud.py)
    * [Cloud Kafka Consumer example](https://github.com/Alfex4936/kafka-Studies/blob/main/python/src/cloudkarafka/AjouSlackConsumerCloud.py)
    
* Kafka (Java)
    * [Simple Producer](https://github.com/Alfex4936/kafka-Studies/blob/main/src/main/java/csw/kafka/study/lesson1/ProducerDemo.java)
    * [Producer with callback](https://github.com/Alfex4936/kafka-Studies/blob/main/src/main/java/csw/kafka/study/lesson1/ProducerDemoCallBack.java)
    * [Producer with key](https://github.com/Alfex4936/kafka-Studies/blob/main/src/main/java/csw/kafka/study/lesson1/ProducerDemoWithKey.java)
    * [Simple Consumer](https://github.com/Alfex4936/kafka-Studies/blob/main/src/main/java/csw/kafka/study/lesson2/ConsumerDemo.java)
    * [Consumer With a thread](https://github.com/Alfex4936/kafka-Studies/blob/main/src/main/java/csw/kafka/study/lesson2/ConsumerDemoThread.java)
    * [Twitter tweets Producer](https://github.com/Alfex4936/kafka-Studies/blob/main/src/main/java/csw/kafka/study/twitter/TwitterProducerOptimized.java)
    * [Twitter tweets filtering with Kafka Streams](https://github.com/Alfex4936/kafka-Studies/blob/main/kafka-streams-filter-tweets/src/main/java/csw/kafka/streams/StreamsFilterTweets.java)

* Kafka (Go)
    * [Simple Producer](https://github.com/Alfex4936/kafka-Studies/blob/main/go/src/basic_producer/BasicProducer.go)
</td></tr>
</table>

## 설치
[Kafka 다운로드](https://kafka.apache.org/downloads)

*Make sure to Download "Binary"*

## 실행

zookeeper & kafka 서버 실행

```console

WIN10@DESKTOP:~$ zookeeper-server-start config/zookeeper.properties

WIN10@DESKTOP:~$ kafka-server-start config/server.properties

```

## CLI 명령어

1. 토픽 만들기 (파티션=3, 복제 계수=2)
```console
WIN10@DESKTOP:~$ kafka-topics --zookeeper localhost:2181 --topic first-topic --create --partitions 3 --replication-factor 2
```

2. 토픽 목록 보기
```console
WIN10@DESKTOP:~$ kafka-topics --zookeeper localhost:2181 --list

first-topic
second-topic
```

3. 토픽 설정 보기
```console
WIN10@DESKTOP:~$ kafka-topics --zookeeper localhost:2181 --topic first-topic --describe

Topic: first-topic      PartitionCount: 3       ReplicationFactor: 1    Configs:
        Topic: first-topic      Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: first-topic      Partition: 1    Leader: 0       Replicas: 0     Isr: 0
        Topic: first-topic      Partition: 2    Leader: 0       Replicas: 0     Isr: 0
```

4. 토픽 컨슈머 (프로듀서 작동할 때)
```console
WIN10@DESKTOP:~$ kafka-console-consumer --bootstrap-server localhost:9092 --topic first-topic
```

5. 토픽 그룹 이름 설정 및 컨슈머
```console
WIN10@DESKTOP:~$ kafka-console-consumer --bootstrap-server localhost:9092 --topic first-topic --group group-one
```

6. 토픽 컨슈머 그룹 목록
```console
WIN10@DESKTOP:~$ kafka-consumer-groups --bootstrap-server localhost:9092 --list

group-one
```

7. 토픽 오프셋 초기화
```console
WIN10@DESKTOP:~$ kafka-consumer-groups --bootstrap-server localhost:9092 --topic first-topic --group group-one --reset-offsets --to-earliest --execute

GROUP                          TOPIC                          PARTITION  NEW-OFFSET
group-one                      first-topic                    0          0
group-one                      first-topic                    1          0
group-one                      first-topic                    2          0

```
