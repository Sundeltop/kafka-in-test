package com.example;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

@Log4j2
public class KafkaMessageConsumer {

    private final KafkaConsumer<String, String> consumer;

    public KafkaMessageConsumer(String bootstrapServers, List<String> topics) {
        consumer = new KafkaConsumer<>(kafkaConsumerProperties(bootstrapServers));
        consumer.subscribe(topics);
        log.info("Consumer subscribed to {}", Arrays.toString(consumer.subscription().toArray()));
    }

    public String getMessage(@NonNull String key, long timeout) {
        final ConsumerRecords<String, String> records = poll(timeout);

        for (ConsumerRecord<String, String> record : records) {
            logRecord(record);
            if (key.equals(record.key())) {
                close();
                return record.value();
            }
        }
        close();
        throw new RuntimeException("Message not found in topic");
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

    private ConsumerRecords<String, String> poll(long timeout) {
        return consumer.poll(ofMillis(timeout));
    }

    private void close() {
        consumer.unsubscribe();
        consumer.close();
    }

    private void logRecord(ConsumerRecord<String, String> record) {
        log.info("topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
}
