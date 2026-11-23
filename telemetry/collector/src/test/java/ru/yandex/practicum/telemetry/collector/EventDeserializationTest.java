package ru.yandex.practicum.telemetry.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.telemetry.collector.model.ClimateSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.DeviceAddedEvent;
import ru.yandex.practicum.telemetry.collector.model.DeviceRemovedEvent;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.LightSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.MotionSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.ScenarioAddedEvent;
import ru.yandex.practicum.telemetry.collector.model.ScenarioRemovedEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SwitchSensorEvent;
import ru.yandex.practicum.telemetry.collector.model.TemperatureSensorEvent;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class EventDeserializationTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void deserializesAllSensorTypes() throws Exception {
        assertSensor("LIGHT_SENSOR_EVENT", "\"linkQuality\":75,\"luminosity\":59", LightSensorEvent.class);
        assertSensor("TEMPERATURE_SENSOR_EVENT", "\"temperatureC\":21,\"temperatureF\":70", TemperatureSensorEvent.class);
        assertSensor("SWITCH_SENSOR_EVENT", "\"state\":true", SwitchSensorEvent.class);
        assertSensor("CLIMATE_SENSOR_EVENT", "\"temperatureC\":21,\"humidity\":45,\"co2Level\":500", ClimateSensorEvent.class);
        assertSensor("MOTION_SENSOR_EVENT", "\"linkQuality\":80,\"motion\":true,\"voltage\":220", MotionSensorEvent.class);
    }

    @Test
    void deserializesAllHubTypes() throws Exception {
        assertHub("DEVICE_ADDED", "\"id\":\"sensor-1\",\"deviceType\":\"LIGHT_SENSOR\"", DeviceAddedEvent.class);
        assertHub("DEVICE_REMOVED", "\"id\":\"sensor-1\"", DeviceRemovedEvent.class);
        assertHub("SCENARIO_ADDED", "\"name\":\"night\",\"conditions\":[],\"actions\":[]", ScenarioAddedEvent.class);
        assertHub("SCENARIO_REMOVED", "\"name\":\"night\"", ScenarioRemovedEvent.class);
    }

    private void assertSensor(String type, String fields, Class<?> expectedType) throws Exception {
        String json = "{\"id\":\"sensor-1\",\"hubId\":\"hub-1\",\"timestamp\":\"2024-08-06T15:11:24.157Z\","
                + fields + ",\"type\":\"" + type + "\"}";
        SensorEvent event = mapper.readValue(json, SensorEvent.class);
        assertThat(event).isInstanceOf(expectedType);
        assertThat(event.getId()).isEqualTo("sensor-1");
        assertThat(event.getHubId()).isEqualTo("hub-1");
        assertThat(event.getTimestamp()).isEqualTo(Instant.parse("2024-08-06T15:11:24.157Z"));
    }

    private void assertHub(String type, String fields, Class<?> expectedType) throws Exception {
        String json = "{\"hubId\":\"hub-1\",\"timestamp\":\"2024-08-06T15:11:24.157Z\","
                + fields + ",\"type\":\"" + type + "\"}";
        HubEvent event = mapper.readValue(json, HubEvent.class);
        assertThat(event).isInstanceOf(expectedType);
        assertThat(event.getHubId()).isEqualTo("hub-1");
        assertThat(event.getTimestamp()).isEqualTo(Instant.parse("2024-08-06T15:11:24.157Z"));
    }
}
