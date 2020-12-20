package csw.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        String myServer = "192.168.137.232:9093";  // My Hyper-V Server

        // Kafka Streams 설정
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, myServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic (bolt)
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter-tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                // filter for tweets that has a user of 10k followers
                (k, jsonTweet) -> getFollwersFromTweet(jsonTweet) > 10000
        );
        filteredStream.to("famous-tweets");  // Send to a topic

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start our streams app
        kafkaStreams.start();

        // kafka-console-consumer --bootstrap-server localhost:9092 --topic famous-tweets
    }

    private static Integer getFollwersFromTweet(String tweetJson) {
        // gson
        try {
            return JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            // got exceptions
            return 0;
        }
    }
}
