package com.example;


import com.example.dto.User;
import com.github.javafaker.Faker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

@Testcontainers
public class KafkaTest {

    private static final String KAFKA_TOPIC = "test-topic";
    private static final long TIMEOUT = 10000;

    private static KafkaContainer kafkaContainer;

    private static KafkaMessageProducer producer;
    private static KafkaMessageConsumer consumer;

    @BeforeAll
    static void setupKafkaContainer() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        kafkaContainer.start();
    }

    @BeforeEach
    void setupProducerAndConsumer() {
        producer = new KafkaMessageProducer(kafkaContainer.getBootstrapServers());
        consumer = new KafkaMessageConsumer(kafkaContainer.getBootstrapServers(), List.of(KAFKA_TOPIC));
    }

    @Test
    public void testKafkaMessageProducerAndConsumer() {
        final String key = "test-key";

        final User user = new User(new Faker().name().fullName());

        producer.send(KAFKA_TOPIC, key, user);

        final User actualMessage = consumer.getMessage(key, TIMEOUT);
        assertEquals(user, actualMessage);
    }

    @Test
    public void testKafkaMessageConsumerEmptyMessage() {
        final String key = "test-key";

        final RuntimeException exception = assertThrowsExactly(RuntimeException.class,
                () -> consumer.getMessage(key, TIMEOUT));
        assertEquals("Message not found in topic", exception.getMessage());
    }

    @AfterAll
    static void tearDownKafkaContainer() {
        kafkaContainer.stop();
    }
}
