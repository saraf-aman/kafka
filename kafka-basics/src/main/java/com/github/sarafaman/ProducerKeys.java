package com.github.sarafaman;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Logger");
        //Create Producer Properties
        Properties properties = new Properties();
        //connects to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 30; i++) {
                String topic = "Topic1";
                String key = "id_" + i;
                String value = "Value_" + i;
                //Create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //Send Data
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // executes everytime a record is sent successfully or an exception is thrown
                    if (e == null) {
                        log.info("Key: " + key + "| Partition: " + recordMetadata.partition());
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