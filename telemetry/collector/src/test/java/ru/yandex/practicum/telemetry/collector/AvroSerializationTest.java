package ru.yandex.practicum.telemetry.collector;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSerializationTest {
    @Test
    void producesRawBinaryAvroReadableBySpecificDatumReader() throws Exception {
        SensorEventAvro source = new SensorEventAvro(
                "sensor-1", "hub-1", Instant.parse("2024-08-06T15:11:24.157Z"), new SwitchSensorAvro(true));

        byte[] bytes = new GeneralAvroSerializer().serialize("telemetry.sensors.v1", source);
        SensorEventAvro restored = new SpecificDatumReader< ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro>(
                SensorEventAvro.getClassSchema()).read(null, DecoderFactory.get().binaryDecoder(bytes, null));

        assertThat(restored.getId()).isEqualTo(source.getId());
        assertThat(restored.getHubId()).isEqualTo(source.getHubId());
        assertThat(restored.getTimestamp()).isEqualTo(source.getTimestamp());
        assertThat(((SwitchSensorAvro) restored.getPayload()).getState()).isTrue();
    }
}
