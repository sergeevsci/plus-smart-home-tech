package ru.yandex.practicum.telemetry.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class EventPublisher {
    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final Producer<String, SpecificRecordBase> producer;
    private final Map<EventTopic, String> topics;

    public EventPublisher(
            Producer<String, SpecificRecordBase> producer,
            @Value("${collector.kafka.topics.sensors}") String sensorTopic,
            @Value("${collector.kafka.topics.hubs}") String hubTopic) {
        this.producer = producer;
        this.topics = new EnumMap<>(EventTopic.class);
        this.topics.put(EventTopic.SENSOR_EVENTS, sensorTopic);
        this.topics.put(EventTopic.HUB_EVENTS, hubTopic);
    }

    public CompletableFuture<Void> publish(
            EventTopic eventTopic,
            String hubId,
            Instant timestamp,
            SpecificRecordBase event) {
        String topic = topics.get(eventTopic);
        CompletableFuture<Void> result = new CompletableFuture<>();
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic,
                null,
                timestamp.toEpochMilli(),
                hubId,
                event
        );

        try {
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    result.complete(null);
                    return;
                }

                log.error("Failed to publish event to Kafka topic {}", topic, exception);
                result.completeExceptionally(new KafkaPublishException(
                        "Kafka did not acknowledge the event", exception));
            });
        } catch (RuntimeException exception) {
            log.error("Failed to send event to Kafka topic {}", topic, exception);
            result.completeExceptionally(new KafkaPublishException(
                    "Kafka did not accept the event", exception));
        }

        return result;
    }
}
