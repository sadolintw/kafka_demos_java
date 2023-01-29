package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        log.info("HelloWorld");

        String conduktorUser = System.getenv("CONDUKTOR_USER");
        String conduktorPassword = System.getenv("CONDUKTOR_PASSWORD");
        System.out.println("CONDUKTOR_USER "+conduktorUser);
        System.out.println("CONDUKTOR_PASSWORD "+conduktorPassword);
        // create Producer Properties
        Properties properties = new Properties();
        // connect to Conduktor
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", conduktorUser, conduktorPassword));
        properties.setProperty("sasl.mechanism", "PLAIN");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        System.out.println("properties "+properties);
        // create the Producer
        log.info("create the Producer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java","dev", "hello world "+sdf.format(new Date()));

        // send data
        log.info("send data");
        producer.send(producerRecord);

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();


    }
}
