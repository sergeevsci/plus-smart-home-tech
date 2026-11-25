package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.CollectorService;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/events")
public class EventController {
    private final CollectorService collectorService;

    public EventController(CollectorService collectorService) {
        this.collectorService = collectorService;
    }

    @PostMapping("/sensors")
    public CompletableFuture<ResponseEntity<Void>> collectSensor(@Valid @RequestBody SensorEvent event) {
        return collectorService.collectSensor(event).thenApply(ignored -> ResponseEntity.ok().build());
    }

    @PostMapping("/hubs")
    public CompletableFuture<ResponseEntity<Void>> collectHub(@Valid @RequestBody HubEvent event) {
        return collectorService.collectHub(event).thenApply(ignored -> ResponseEntity.ok().build());
    }
}
