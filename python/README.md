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

