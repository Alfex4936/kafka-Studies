package csw.kafka.study.lesson2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    public static void main(String[] args) {
        new ConsumerDemoThread().run();
    }

    private ConsumerDemoThread() {}

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());
        final String myServer = "localhost:9092";
        final String groupId = "group-one";
        final String topic = "first-topic";

        // 멀티 쓰레드를 위한 latch
        final CountDownLatch latch = new CountDownLatch(1);

        // Consumer runnable 만들기
        logger.info("Creating the consumer thread.");

        Runnable myConsumerRunnable = new ConsumerRunnable(
                myServer,
                groupId,
                topic,
                latch
        );

        // 쓰레드 시작
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // 종료 hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {  // lambda JDK 8+
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited.");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupterd:", e);
        } finally {
            logger.info("Application is closing.");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServer,
                              String groupId,
                              String topic,
                              CountDownLatch latch) {
            this.latch = latch;
            // Kafka Consumer 설정
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // earliest, latest, none

            // Kafka Consumer 만들기
            consumer = new KafkaConsumer<>(properties);

            // topic에 연결
            consumer.subscribe(Collections.singleton(topic));
            // consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            // Poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                        logger.info(String.format("Partition:  %s, Offset: %s", record.partition(), record.offset()));
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // 메인 코드 종료
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
