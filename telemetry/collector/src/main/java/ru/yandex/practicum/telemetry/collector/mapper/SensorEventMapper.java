package ru.yandex.practicum.telemetry.collector.mapper;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.ClimateSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.LightSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.MotionSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SwitchSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.TemperatureSensorEvent;

@Component
public class SensorEventMapper {
    public SensorEventAvro map(SensorEvent event) {
        Object payload = switch (event.getType()) {
            case LIGHT_SENSOR_EVENT -> mapLight((LightSensorEvent) event);
            case TEMPERATURE_SENSOR_EVENT -> mapTemperature((TemperatureSensorEvent) event);
            case SWITCH_SENSOR_EVENT -> mapSwitch((SwitchSensorEvent) event);
            case CLIMATE_SENSOR_EVENT -> mapClimate((ClimateSensorEvent) event);
            case MOTION_SENSOR_EVENT -> mapMotion((MotionSensorEvent) event);
        };
        return new SensorEventAvro(event.getId(), event.getHubId(), event.getTimestamp(), payload);
    }

    private LightSensorAvro mapLight(LightSensorEvent event) {
        return new LightSensorAvro(event.getLinkQuality(), event.getLuminosity());
    }

    private TemperatureSensorAvro mapTemperature(TemperatureSensorEvent event) {
        return new TemperatureSensorAvro(event.getTemperatureC(), event.getTemperatureF());
    }

    private SwitchSensorAvro mapSwitch(SwitchSensorEvent event) {
        return new SwitchSensorAvro(event.isState());
    }

    private ClimateSensorAvro mapClimate(ClimateSensorEvent event) {
        return new ClimateSensorAvro(event.getTemperatureC(), event.getHumidity(), event.getCo2Level());
    }

    private MotionSensorAvro mapMotion(MotionSensorEvent event) {
        return new MotionSensorAvro(event.getLinkQuality(), event.isMotion(), event.getVoltage());
    }
}
