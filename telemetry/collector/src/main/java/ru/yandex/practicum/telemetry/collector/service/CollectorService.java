package ru.yandex.practicum.telemetry.collector.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.kafka.EventPublisher;
import ru.yandex.practicum.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;

import java.util.concurrent.CompletableFuture;

@Service
public class CollectorService {
    private final SensorEventMapper sensorMapper;
    private final HubEventMapper hubMapper;
    private final EventPublisher publisher;

    public CollectorService(SensorEventMapper sensorMapper, HubEventMapper hubMapper, EventPublisher publisher) {
        this.sensorMapper = sensorMapper;
        this.hubMapper = hubMapper;
        this.publisher = publisher;
    }

    public CompletableFuture<Void> collectSensor(SensorEvent event) {
        return publisher.publishSensor(
                event.getHubId(),
                event.getTimestamp(),
                sensorMapper.map(event)
        );
    }

    public CompletableFuture<Void> collectHub(HubEvent event) {
        return publisher.publishHub(
                event.getHubId(),
                event.getTimestamp(),
                hubMapper.map(event)
        );
    }
}
