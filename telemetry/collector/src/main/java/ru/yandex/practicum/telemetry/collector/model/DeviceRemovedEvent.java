package ru.yandex.practicum.telemetry.collector.model;

import jakarta.validation.constraints.NotBlank;

public final class DeviceRemovedEvent extends HubEvent {
    @NotBlank
    private String id;

    @Override
    public HubEventType getType() { return HubEventType.DEVICE_REMOVED; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
}
