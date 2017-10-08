package com.dexter.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {

    private KafkaProducer<Integer, String> producer;
    private final Properties properties = new Properties();

    public SimpleProducer() {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        SimpleProducer simpleProducer = new SimpleProducer();
        String topic = "topic";
        String msg = "message";
        ProducerRecord<Integer, String> data = new ProducerRecord<>(topic, 8, msg);
        simpleProducer.producer.send(data);
        simpleProducer.producer.close();
    }

}
