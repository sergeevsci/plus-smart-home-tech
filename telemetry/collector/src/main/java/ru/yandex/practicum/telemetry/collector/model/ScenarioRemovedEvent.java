package ru.yandex.practicum.telemetry.collector.model;

import jakarta.validation.constraints.NotBlank;

public final class ScenarioRemovedEvent extends HubEvent {
    @NotBlank
    private String name;

    @Override
    public HubEventType getType() { return HubEventType.SCENARIO_REMOVED; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
