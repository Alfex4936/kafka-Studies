package csw.kafka.study.lesson1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String myServer = "localhost:9092";

        // Kafka Producer 설정
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Kafka는 데이터를 바이트로 보냄 (0,1)

        // Kafka Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "Hello World!");

        // Kafka Producer 만들기
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Consumer한테 데이터 보내기 - 비동기
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
