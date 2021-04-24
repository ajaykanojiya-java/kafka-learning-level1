package com.github.ajayonjava;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        final String bootstrap_server = "localhost:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record: what message to send
        ProducerRecord<String,String> record =
                new ProducerRecord<String, String>("first_topic","Hello World from Kafka Producer");

        //send date - asynchronous
        producer.send(record);
        //flush data - if not flush and close then consumer can not consume messages
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
