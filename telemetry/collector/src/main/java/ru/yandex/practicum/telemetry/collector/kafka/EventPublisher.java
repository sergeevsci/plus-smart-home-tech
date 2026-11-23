package ru.yandex.practicum.telemetry.collector.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Component
public class EventPublisher {
    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;
    private final String sensorTopic;
    private final String hubTopic;

    public EventPublisher(
            KafkaTemplate<String, SpecificRecordBase> kafkaTemplate,
            @Value("${collector.kafka.topics.sensors}") String sensorTopic,
            @Value("${collector.kafka.topics.hubs}") String hubTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.sensorTopic = sensorTopic;
        this.hubTopic = hubTopic;
    }

    public CompletableFuture<Void> publishSensor(String hubId, SpecificRecordBase event) {
        return publish(sensorTopic, hubId, event);
    }

    public CompletableFuture<Void> publishHub(String hubId, SpecificRecordBase event) {
        return publish(hubTopic, hubId, event);
    }

    private CompletableFuture<Void> publish(String topic, String key, SpecificRecordBase event) {
        return kafkaTemplate.send(topic, key, event)
                .thenApply(result -> (Void) null)
                .exceptionally(exception -> {
                    Throwable cause = exception instanceof CompletionException && exception.getCause() != null
                            ? exception.getCause() : exception;
                    log.error("Failed to publish event to Kafka topic {}", topic, cause);
                    throw new CompletionException(new KafkaPublishException(
                            "Kafka did not acknowledge the event", cause));
                });
    }
}
