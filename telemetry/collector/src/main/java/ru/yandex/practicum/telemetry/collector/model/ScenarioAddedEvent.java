package ru.yandex.practicum.telemetry.collector.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

public final class ScenarioAddedEvent extends HubEvent {
    @NotBlank
    @Size(min = 3)
    private String name;
    @NotEmpty
    private List<@Valid ScenarioCondition> conditions;
    @NotEmpty
    private List<@Valid DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<ScenarioCondition> getConditions() {
        return conditions;
    }
    public void setConditions(List<ScenarioCondition> conditions) {
        this.conditions = conditions;
    }
    public List<DeviceAction> getActions() {
        return actions;
    }
    public void setActions(List<DeviceAction> actions) {
        this.actions = actions;
    }
}
