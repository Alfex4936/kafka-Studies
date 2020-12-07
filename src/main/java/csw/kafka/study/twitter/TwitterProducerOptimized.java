package csw.kafka.study.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerOptimized {
    final Logger logger = LoggerFactory.getLogger(TwitterProducerOptimized.class.getName());

    public TwitterProducerOptimized() {
        // pass
    }

    public static void main(String[] args) {
        new TwitterProducerOptimized().run();
    }

    public void run() {
        logger.info("Setting up...");

        // 트위터 클라이언트
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterProducer(msgQueue);
        client.connect();

        // Kafka Producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // 종료 hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            client.stop();
            producer.close();
            logger.info("Bye!");
        }));

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                // Kafka에 트윗 전송하기
                // kafka-topics --zookeeper localhost:2181 --create --topic twitter-tweets --partitions 6 --replication-factor 1
                producer.send(new ProducerRecord<>("twitter-tweets", null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        logger.info("Something bad happended", e);
                    }
                });
            }
        }
        logger.info("End of application.");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String myServer = "localhost:9092";

        // Kafka Producer 설정
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, myServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe Producer 설정
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput Producer 설정
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // Kafka Producer 만들기
        return new KafkaProducer<>(properties);
    }

    public Client createTwitterProducer(BlockingQueue<String> msgQueue) {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) //
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka", "korea");  // tweets about kafka
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        final File fileName = new File("src/main/java/csw/kafka/study/twitter/app.config");
        final Properties prop = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(fileName.getAbsolutePath());
        } catch (FileNotFoundException ex) {
            logger.error("app.config not found." + fileName.getAbsolutePath());
            // do nothing
        }

        try {
            prop.load(is);
        } catch (IOException ex) {
            logger.error("app.config load failed.");
            // do nothing
        }

        String consumerSecret = prop.getProperty("consumerSecret");
        String consumerKey = prop.getProperty("consumerKey");
        String token = prop.getProperty("token");
        String tokenSecret = prop.getProperty("tokenSecret");

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
