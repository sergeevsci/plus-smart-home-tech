package ru.yandex.practicum.telemetry.collector.model;

import jakarta.validation.constraints.NotNull;

public final class SwitchSensorEvent extends SensorEvent {
    @NotNull
    private Boolean state;

    @Override
    public SensorEventType getType() { return SensorEventType.SWITCH_SENSOR_EVENT; }

    public Boolean getState() { return state; }
    public void setState(Boolean state) { this.state = state; }
}
