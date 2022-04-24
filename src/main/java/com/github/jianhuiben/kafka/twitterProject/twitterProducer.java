package com.github.jianhuiben.kafka.twitterProject;

import com.github.jianhuiben.kafka.quickStart.ProducerDemoKeys;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class twitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(twitterProducer.class);
    public twitterProducer() {}

    public static void main(String[] args) throws IOException, URISyntaxException {
        new twitterProducer().run();
    }

    public void run() throws IOException, URISyntaxException {
        // set up rules
        Map<String, String> rules = new HashMap<>();
        rules.put("cats has:images", "cat images");
        rules.put("dogs has:images", "dog images");

        // create a twitter client
        BufferedReader StreamTweetsReader = new twitterStreamClient().getStreamTweets(rules);

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            producer.close();
            logger.info("Done!");
        }));

        // loop to send tweets to kafka
        sendTweetsToProducer(StreamTweetsReader, producer);
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "localhost:9092";
        Logger logger = LoggerFactory.getLogger(String.valueOf(ProducerDemoKeys.class));
        // create producer properties https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public void sendTweetsToProducer(BufferedReader StreamTweetsReader, KafkaProducer<String, String> producer) throws IOException {
        String tweet = StreamTweetsReader.readLine();
        while (tweet != null) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("tweetsTopic", tweet);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time a record is successfully sent or an exception is thrown
                    if(e == null) {
                        // the record was successfully sent
                        logger.info("Recevied new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
            //flush the data
            producer.flush();
            tweet = StreamTweetsReader.readLine();
        }
    }
}
