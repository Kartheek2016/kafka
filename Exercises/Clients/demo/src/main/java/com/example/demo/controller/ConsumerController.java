package com.example.demo.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Properties;

@RestController
public class ConsumerController {

    @PostMapping("/consumer/{topic}")
    public void initateConsmer(@PathVariable("topic") String topicName) {
        if(StringUtils.isEmpty(topicName)) {
            System.out.println("Enter topic name");
            return;
        }

        // Kafka consumer configuration settings
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer <String, String>(props);
        // Kafka Consumer subscribes list of topics here.
        consumer.subscribe(List.of(topicName));

        // print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}
