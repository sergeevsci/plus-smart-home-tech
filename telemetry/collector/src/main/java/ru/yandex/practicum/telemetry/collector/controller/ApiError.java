package ru.yandex.practicum.telemetry.collector.controller;

import java.time.Instant;

public record ApiError(Instant timestamp, int status, String message) {
}
