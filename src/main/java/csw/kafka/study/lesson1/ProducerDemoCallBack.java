package csw.kafka.study.lesson1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class);
        String myServer = "localhost:9092";

        // Kafka Producer 설정
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Kafka는 데이터를 바이트로 보냄 (0,1)

        // Kafka Producer 만들기
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 5; i++) {
            // Kafka Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "Hello " + i);

            // Consumer한테 데이터 보내기 - 비동기
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // 레코드가 잘 보내졌거나, 오류가 발생할 때마다 실행됨.
                    if (e == null) {
                        // 레코드 전송 성공
                        final String log = String.format("새 메타데이터%nTopic: %s%nPartition: %d%nOffset: %d%nTimestamp: %d",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                        logger.info(log);

                        /* 예)
                        새 메타데이터
                        Topic: first-topic
                        Partition: 2
                        Offset: 13
                        Timestamp: 1606555474565
                         */
                    } else {
                        logger.info(String.format("오류가 발생함: %s", e));
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
