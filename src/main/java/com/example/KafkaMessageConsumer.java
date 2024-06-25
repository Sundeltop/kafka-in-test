package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class KafkaMessageConsumer {

    private final KafkaConsumer<String, String> consumer;

    public KafkaMessageConsumer(String bootstrapServers, List<String> topics) {
        consumer = new KafkaConsumer<>(kafkaConsumerProperties(bootstrapServers));
        consumer.subscribe(topics);
    }

    public ConsumerRecords<String, String> poll(long timeout) {
        return consumer.poll(ofMillis(timeout));
    }

    public void close() {
        consumer.close();
    }

    private Properties kafkaConsumerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(GROUP_ID_CONFIG, "KafkaMessageConsumer");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }
}
