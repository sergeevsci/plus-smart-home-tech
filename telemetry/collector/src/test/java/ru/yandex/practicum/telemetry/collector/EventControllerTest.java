package ru.yandex.practicum.telemetry.collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ru.yandex.practicum.telemetry.collector.controller.ApiExceptionHandler;
import ru.yandex.practicum.telemetry.collector.controller.EventController;
import ru.yandex.practicum.telemetry.collector.service.CollectorService;

import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class EventControllerTest {
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        mockMvc = MockMvcBuilders.standaloneSetup(new EventController(mock(CollectorService.class)))
                .setControllerAdvice(new ApiExceptionHandler())
                .setMessageConverters(new org.springframework.http.converter.json.MappingJackson2HttpMessageConverter(mapper))
                .build();
    }

    @Test
    void rejectsMalformedJson() throws Exception {
        mockMvc.perform(post("/events/sensors").contentType(MediaType.APPLICATION_JSON).content("{broken"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsUnknownEventType() throws Exception {
        mockMvc.perform(post("/events/sensors").contentType(MediaType.APPLICATION_JSON).content("""
                {"id":"sensor-1","hubId":"hub-1","timestamp":"2024-08-06T15:11:24Z","type":"UNKNOWN"}
                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsUnknownHubScenarioEventType() throws Exception {
        mockMvc.perform(post("/events/hubs").contentType(MediaType.APPLICATION_JSON).content("""
                {"hubId":"hub-1","name":"unknown scenario","type":"SCENARIO_UPDATED"}
                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsMissingRequiredSensorReading() throws Exception {
        mockMvc.perform(post("/events/sensors").contentType(MediaType.APPLICATION_JSON).content("""
                {"id":"motion-1","hubId":"hub-1","motion":true,"voltage":220,
                 "type":"MOTION_SENSOR_EVENT"}
                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void rejectsShortScenarioNameAndEmptyCollections() throws Exception {
        mockMvc.perform(post("/events/hubs").contentType(MediaType.APPLICATION_JSON).content("""
                {"hubId":"hub-1","name":"ab","conditions":[],"actions":[],"type":"SCENARIO_ADDED"}
                """))
                .andExpect(status().isBadRequest());
    }
}
