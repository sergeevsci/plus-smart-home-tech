package ru.yandex.practicum.telemetry.collector;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.collector.kafka.EventPublisher;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaPublishException;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class EventPublisherTest {

    @Test
    void routesEventsWithHubKeyToConfiguredTopics() {
        Producer<String, SpecificRecordBase> producer = successfulProducer();
        SpecificRecordBase event = new SwitchSensorAvro(true);
        EventPublisher publisher = new EventPublisher(producer, "sensors-topic", "hubs-topic");
        Instant sensorTimestamp = Instant.parse("2024-08-06T15:11:24Z");
        Instant hubTimestamp = Instant.parse("2024-08-06T15:12:24Z");

        publisher.publishSensor("hub-1", sensorTimestamp, event).join();
        publisher.publishHub("hub-2", hubTimestamp, event).join();

        ArgumentCaptor<ProducerRecord<String, SpecificRecordBase>> captor =
                ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, times(2)).send(captor.capture(), any(Callback.class));

        assertThat(captor.getAllValues())
                .extracting(ProducerRecord::topic, ProducerRecord::key, ProducerRecord::timestamp)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(
                                "sensors-topic",
                                "hub-1",
                                sensorTimestamp.toEpochMilli()
                        ),
                        org.assertj.core.groups.Tuple.tuple(
                                "hubs-topic",
                                "hub-2",
                                hubTimestamp.toEpochMilli()
                        )
                );
    }

    @Test
    void exposesKafkaAcknowledgementFailure() {
        Producer<String, SpecificRecordBase> producer = mock(Producer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenAnswer(invocation -> {
                    Callback callback = invocation.getArgument(1);
                    callback.onCompletion(null, new RuntimeException("broker unavailable"));
                    return CompletableFuture.completedFuture(null);
                });
        SpecificRecordBase event = new SwitchSensorAvro(true);
        EventPublisher publisher = new EventPublisher(producer, "sensors-topic", "hubs-topic");

        assertThatThrownBy(() -> publisher.publishSensor("hub-1", Instant.now(), event).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(KafkaPublishException.class);
    }

    private Producer<String, SpecificRecordBase> successfulProducer() {
        Producer<String, SpecificRecordBase> producer = mock(Producer.class);
        when(producer.send(any(ProducerRecord.class), any(Callback.class)))
                .thenAnswer(invocation -> {
                    Callback callback = invocation.getArgument(1);
                    callback.onCompletion(null, null);
                    return CompletableFuture.completedFuture(null);
                });
        return producer;
    }
}
