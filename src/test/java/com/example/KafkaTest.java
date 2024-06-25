package com.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.rnorth.ducttape.unreliables.Unreliables.retryUntilTrue;

@Testcontainers
public class KafkaTest {

    private static final String KAFKA_TOPIC = "test-topic";

    private static KafkaContainer kafkaContainer;

    private static KafkaMessageProducer producer;
    private static KafkaMessageConsumer consumer;

    @BeforeAll
    static void setUp() {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
        kafkaContainer.start();

        producer = new KafkaMessageProducer(kafkaContainer.getBootstrapServers());
        consumer = new KafkaMessageConsumer(kafkaContainer.getBootstrapServers(), List.of(KAFKA_TOPIC));
    }

    @Test
    public void testKafkaMessageProducerAndConsumer() {
        final String key = "test-key";
        final String value = "test-value";

        producer.send(KAFKA_TOPIC, key, value);

        retryUntilTrue(10, SECONDS, () -> {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.isEmpty()) {
                return false;
            }
            for (ConsumerRecord<String, String> record : records) {
                assertEquals(key, record.key());
                assertEquals(value, record.value());
            }
            return true;
        });
    }

    @AfterAll
    static void tearDown() {
        producer.close();
        consumer.close();
        kafkaContainer.stop();
    }
}
