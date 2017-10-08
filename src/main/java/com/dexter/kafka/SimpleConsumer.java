package com.dexter.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class SimpleConsumer {

    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;

    public SimpleConsumer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    public void testConsumer() {

        consumer.subscribe(Collections.singleton(topic));

        while(true){
            ConsumerRecords<Integer, String> poll = consumer.poll(100);
            for(ConsumerRecord r : poll) {
                System.out.println("key : " + r.key());
                System.out.println(r);
            }
        }

    }

    public static void main(String[] args) {
        String topic = "topic";
        SimpleConsumer simpleHLConsumer = new SimpleConsumer(topic);
        simpleHLConsumer.testConsumer();
    }

}