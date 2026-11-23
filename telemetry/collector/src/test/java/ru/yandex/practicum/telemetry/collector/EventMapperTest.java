package ru.yandex.practicum.telemetry.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.mapper.HubEventMapper;
import ru.yandex.practicum.telemetry.collector.mapper.SensorEventMapper;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class EventMapperTest {
    private final ObjectMapper json = new ObjectMapper().findAndRegisterModules();

    @Test
    void mapsSensorIdentityTimestampAndPayload() throws Exception {
        SensorEvent input = json.readValue("""
                {"id":"sensor.light.3","hubId":"hub-2","timestamp":"2024-08-06T15:11:24.157Z",
                 "linkQuality":75,"luminosity":59,"type":"LIGHT_SENSOR_EVENT"}
                """, SensorEvent.class);

        SensorEventAvro result = new SensorEventMapper().map(input);

        assertThat(result.getId()).isEqualTo("sensor.light.3");
        assertThat(result.getHubId()).isEqualTo("hub-2");
        assertThat(result.getTimestamp()).isEqualTo(Instant.parse("2024-08-06T15:11:24.157Z"));
        assertThat(result.getPayload()).isInstanceOf(LightSensorAvro.class);
        assertThat(((LightSensorAvro) result.getPayload()).getLuminosity()).isEqualTo(59);
    }

    @Test
    void mapsEverySensorPayloadType() throws Exception {
        assertSensorPayload("LIGHT_SENSOR_EVENT", "\"linkQuality\":75,\"luminosity\":59", LightSensorAvro.class);
        assertSensorPayload("TEMPERATURE_SENSOR_EVENT", "\"temperatureC\":21,\"temperatureF\":70", TemperatureSensorAvro.class);
        assertSensorPayload("SWITCH_SENSOR_EVENT", "\"state\":true", SwitchSensorAvro.class);
        assertSensorPayload("CLIMATE_SENSOR_EVENT", "\"temperatureC\":21,\"humidity\":45,\"co2Level\":500", ClimateSensorAvro.class);
        assertSensorPayload("MOTION_SENSOR_EVENT", "\"linkQuality\":80,\"motion\":true,\"voltage\":220", MotionSensorAvro.class);
    }

    @Test
    void mapsScenarioCollectionsIntegerAndNullableValues() throws Exception {
        HubEvent input = json.readValue("""
                {"hubId":"hub-1","timestamp":"2024-08-06T15:11:24.157Z","type":"SCENARIO_ADDED",
                 "name":"night","conditions":[
                   {"sensorId":"motion-1","type":"MOTION","operation":"EQUALS","value":1},
                   {"sensorId":"light-1","type":"LUMINOSITY","operation":"LOWER_THAN","value":null}],
                 "actions":[{"sensorId":"switch-1","type":"ACTIVATE","value":null}]}
                """, HubEvent.class);

        HubEventAvro result = new HubEventMapper().map(input);
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) result.getPayload();
        ScenarioConditionAvro numericCondition = payload.getConditions().getFirst();
        DeviceActionAvro action = payload.getActions().getFirst();

        assertThat(result.getHubId()).isEqualTo("hub-1");
        assertThat(result.getTimestamp()).isEqualTo(Instant.parse("2024-08-06T15:11:24.157Z"));
        assertThat(numericCondition.getValue()).isEqualTo(1);
        assertThat(payload.getConditions().get(1).getValue()).isNull();
        assertThat(action.getValue()).isNull();
    }

    @Test
    void mapsEveryHubPayloadType() throws Exception {
        assertHubPayload("DEVICE_ADDED", "\"id\":\"sensor-1\",\"deviceType\":\"LIGHT_SENSOR\"", DeviceAddedEventAvro.class);
        assertHubPayload("DEVICE_REMOVED", "\"id\":\"sensor-1\"", DeviceRemovedEventAvro.class);
        assertHubPayload("SCENARIO_ADDED", "\"name\":\"night\",\"conditions\":[],\"actions\":[]", ScenarioAddedEventAvro.class);
        assertHubPayload("SCENARIO_REMOVED", "\"name\":\"night\"", ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro.class);
    }

    private void assertSensorPayload(String type, String fields, Class<?> expectedPayload) throws Exception {
        SensorEvent event = json.readValue("{\"id\":\"sensor-1\",\"hubId\":\"hub-1\","
                + "\"timestamp\":\"2024-08-06T15:11:24.157Z\"," + fields + ",\"type\":\"" + type + "\"}",
                SensorEvent.class);
        assertThat(new SensorEventMapper().map(event).getPayload()).isInstanceOf(expectedPayload);
    }

    private void assertHubPayload(String type, String fields, Class<?> expectedPayload) throws Exception {
        HubEvent event = json.readValue("{\"hubId\":\"hub-1\",\"timestamp\":\"2024-08-06T15:11:24.157Z\","
                + fields + ",\"type\":\"" + type + "\"}", HubEvent.class);
        assertThat(new HubEventMapper().map(event).getPayload()).isInstanceOf(expectedPayload);
    }
}
