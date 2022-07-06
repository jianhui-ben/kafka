package com.kafka.kafkaStream;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamFiltersTweets {
    public static Logger logger = LoggerFactory.getLogger(StreamFiltersTweets.class);

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        //like consumer groups, but in kafka streams
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");

        // default set string for key & values for deserialization
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream("TwitterElasticSearch");
        KStream<String, String> filteredStreams = inputTopic.filter(
                // filter tweets that have > 250 characters in length.
                (k, jsonTweet) -> extractTweetsTextLength(jsonTweet) > 250
        );

        filteredStreams.to("LongTweets");

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // starts our stream application
        kafkaStreams.start();

    }

    private static JsonParser jsonParser = new JsonParser();
    public static Integer extractTweetsTextLength (String tweetJson){
        //gson library
        try {
            return jsonParser.parse(tweetJson).getAsJsonObject().get("data")
                    .getAsJsonObject().get("text").getAsString().length();
        }catch (Exception e) {
            logger.info("Tweets: " + tweetJson);
            return 0;
        }
    }
}
