package ru.yandex.practicum.telemetry.collector;

import org.apache.avro.specific.SpecificRecordBase;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.collector.kafka.EventPublisher;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaPublishException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventPublisherTest {
    @Test
    void routesEventsWithHubKeyToConfiguredTopics() {
        KafkaTemplate<String, SpecificRecordBase> kafka = mock(KafkaTemplate.class);
        SpecificRecordBase event = new SwitchSensorAvro(true);
        when(kafka.send("sensors-topic", "hub-1", event)).thenReturn(CompletableFuture.completedFuture(null));
        when(kafka.send("hubs-topic", "hub-2", event)).thenReturn(CompletableFuture.completedFuture(null));
        EventPublisher publisher = new EventPublisher(kafka, "sensors-topic", "hubs-topic");

        publisher.publishSensor("hub-1", event).join();
        publisher.publishHub("hub-2", event).join();

        verify(kafka).send("sensors-topic", "hub-1", event);
        verify(kafka).send("hubs-topic", "hub-2", event);
    }

    @Test
    void exposesKafkaAcknowledgementFailure() {
        KafkaTemplate<String, SpecificRecordBase> kafka = mock(KafkaTemplate.class);
        SpecificRecordBase event = new SwitchSensorAvro(true);
        CompletableFuture failed = CompletableFuture.failedFuture(new RuntimeException("broker unavailable"));
        when(kafka.send("sensors-topic", "hub-1", event)).thenReturn(failed);
        EventPublisher publisher = new EventPublisher(kafka, "sensors-topic", "hubs-topic");

        assertThatThrownBy(() -> publisher.publishSensor("hub-1", event).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(KafkaPublishException.class);
    }
}
