package com.github.sarafaman;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Logger");
        //Create Producer Properties
        Properties properties = new Properties();
        //connects to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //connects to kafka-cluster
//        properties.setProperty("bootstrap.servers", "https://glad-mosquito-11683-us1-kafka.upstash.io:9092");
//        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"Z2xhZC1tb3NxdWl0by0xMTY4MyRBRw76_CRUs4CwDOm6sSLhGviCHWhXUMSd63s\" password=\"ODliZmIxYzUtMzg5NC00NzI2LTg1OGUtYTg5ZTlkNTdiMjEw\";");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                //Create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("Topic1", "Iteration Value: " + j);
                //Send Data
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // executes everytime a record is sent successfully or an exception is thrown
                    if (e == null) {
                        log.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n" );
                    }
                    else {
                        log.error("Error while producing: ", e);
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //Flush - sends all data block until done -- synchronous
        producer.flush();
        //Close the Producer
        producer.close();
    }
}