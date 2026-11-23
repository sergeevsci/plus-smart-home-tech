package ru.yandex.practicum.telemetry.collector.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaPublishException;

import java.time.Instant;
import java.util.concurrent.CompletionException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentNotValidException.class,
            IllegalArgumentException.class})
    public ResponseEntity<ApiError> handleBadRequest(Exception exception) {
        return response(HttpStatus.BAD_REQUEST, "Invalid event payload");
    }

    @ExceptionHandler(KafkaPublishException.class)
    public ResponseEntity<ApiError> handleKafkaFailure(KafkaPublishException exception) {
        return response(HttpStatus.SERVICE_UNAVAILABLE, exception.getMessage());
    }

    @ExceptionHandler(CompletionException.class)
    public ResponseEntity<ApiError> handleCompletionFailure(CompletionException exception) {
        if (exception.getCause() instanceof KafkaPublishException kafkaException) {
            return handleKafkaFailure(kafkaException);
        }
        return response(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to process event");
    }

    private ResponseEntity<ApiError> response(HttpStatus status, String message) {
        return ResponseEntity.status(status)
                .body(new ApiError(Instant.now(), status.value(), message));
    }
}
