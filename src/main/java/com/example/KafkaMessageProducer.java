package com.example;

import com.example.dto.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Log4j2
public class KafkaMessageProducer {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;

    public KafkaMessageProducer(String bootstrapServers) {
        this.producer = new KafkaProducer<>(kafkaProducerProperties(bootstrapServers));
        mapper = new ObjectMapper();
    }

    @SneakyThrows
    public void send(String topic, String key, User user) {
        producer.send(new ProducerRecord<>(topic, key, mapper.writeValueAsString(user)));
        log.info("Produce Kafka Message with key: {}, value: {}", key, user);
    }

    public void close() {
        producer.close();
    }

    private Properties kafkaProducerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
