package com.github.sarafaman;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Logger");

        String groupId = "my-first-group";
        String topic = "Topic1";

        //Create Producer Properties
        Properties properties = new Properties();
        //connects to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //connects to kafka-cluster
//        properties.setProperty("bootstrap.servers", "https://glad-mosquito-11683-us1-kafka.upstash.io:9092");
//        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2xhZC1tb3NxdWl0by0xMTY4MyRBRw76_CRUs4CwDOm6sSLhGviCHWhXUMSd63s\" password=\"ODliZmIxYzUtMzg5NC00NzI2LTg1OGUtYTg5ZTlkNTdiMjEw\";");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest"); //there are three option: none, earliest, latest

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + " | Value: " + record.value());
                log.info("Partition: " + record.partition() + " | Offset: " + record.offset());
            }
        }
    }
}