package com.github.ajayonjava;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);

    public static void main(String[] args) {
        System.out.println("start..1");
        final String bootstrap_server = "localhost:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record: what message to send
        for(int i=0;i<10;i++){
            ProducerRecord<String,String> record =
                    new ProducerRecord<>("first_topic","Hello World from Kafka Producer "+i);

            //send date - asynchronous
            producer.send(record, (recordMetadata, exception) ->{
                if(exception==null){
                    logger.info("Received new metadata \n");
                    logger.info("Topic: "+recordMetadata.topic()+"\n");
                    logger.info("Partition: "+recordMetadata.partition()+"\n");
                    logger.info("Offset: "+recordMetadata.offset()+"\n");
                    logger.info("Timestamp: "+recordMetadata.timestamp());
                }
                else{
                    logger.error("Error while producing..",exception);
                }
            });
        }

        //flush data - if not flush and close then consumer can not consume messages
        producer.flush();
        //flush and close producer
        producer.close();
        System.out.println("ends..1");
    }
}
