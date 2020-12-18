<div align="center">
<p>
    <img width="480" src="https://www.andplus.com/hs-fs/hubfs/kafkalogo.jpg?&name=kafkalogo.jpg">
</p>
<h1>Apache Kafka 공부 (Java, Python, Go)</h1>
    <h5>v2.13-2.6.0</h5>

[Apache Kafka](https://kafka.apache.org/)

</div>

## Twitter Kafka Connect

[Github Link](https://github.com/jcustenborder/kafka-connect-twitter)

1. 토픽 만들기 (파티션=3, 복제 계수=1)
```console
WIN10@DESKTOP:~$ kafka-topics --zookeeper localhost:2181 --topic twitter-status-connect --create --partitions 3 --replication-factor 1
WIN10@DESKTOP:~$ kafka-topics --zookeeper localhost:2181 --topic twitter-delete-connect --create --partitions 3 --replication-factor 1
```

2. 트위터 컨슈머 (프로듀서 작동할 때)
```console
WIN10@DESKTOP:~$ kafka-console-consumer --bootstrap-server localhost:9092 --topic twitter-status-connect --from-beginning
```

2. 트위터 프로듀서
```console
WIN10@DESKTOP:~$ connect-standalone connect-standalone.properties twitter.properties
```