package csw.kafka.study.lesson2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        final String myServer = "localhost:9092";
        final String groupId = "group-one";
        final String topic = "first-topic";

        // Kafka Consumer 설정
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, myServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // earliest, latest, none

        // Kafka Consumer 만들기
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Assign
        TopicPartition partitionReadFrom = new TopicPartition(topic, 0);
        final long offsetReadFrom = 5L;
        consumer.assign(Collections.singletonList(partitionReadFrom));

        // Seek
        consumer.seek(partitionReadFrom, offsetReadFrom);
        int msgToRead = 5;
        // Poll for new data
        while (msgToRead > 0) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                msgToRead -= 1;
                logger.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                logger.info(String.format("Partition:  %s, Offset: %s", record.partition(), record.offset()));

                if (msgToRead == 0) {
                    break;
                }
            }
        }

        logger.info("Exiting...");
    }
}
