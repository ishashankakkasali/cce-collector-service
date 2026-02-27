package org.openphc.cce.collector.api.exception;

import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Global exception handler — centralizes error responses for all controllers.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(CloudEventValidationException.class)
    public ResponseEntity<ApiResponse<Void>> handleCloudEventValidation(CloudEventValidationException ex) {
        log.warn("CloudEvents validation failed: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.error("VALIDATION_ERROR", ex.getMessage(),
                        Map.of("errors", ex.getErrors())));
    }

    @ExceptionHandler(InvalidEventTypeException.class)
    public ResponseEntity<ApiResponse<Void>> handleInvalidEventType(InvalidEventTypeException ex) {
        log.warn("Invalid event type: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.error("INVALID_EVENT_TYPE", ex.getMessage(),
                        Map.of("type", ex.getEventType())));
    }

    @ExceptionHandler(UnsupportedContentTypeException.class)
    public ResponseEntity<ApiResponse<Void>> handleUnsupportedContentType(UnsupportedContentTypeException ex) {
        log.warn("Unsupported content type: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.error("UNSUPPORTED_CONTENT_TYPE", ex.getMessage(),
                        Map.of("errors", ex.getErrors())));
    }

    @ExceptionHandler(FhirValidationException.class)
    public ResponseEntity<ApiResponse<Void>> handleFhirValidation(FhirValidationException ex) {
        log.error("FHIR validation failed: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(ApiResponse.error("FHIR_VALIDATION_ERROR", ex.getMessage(),
                        Map.of("errors", ex.getErrors())));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> handleBeanValidation(MethodArgumentNotValidException ex) {
        String errors = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .collect(Collectors.joining(", "));
        log.warn("Bean validation failed: {}", errors);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ApiResponse.error("VALIDATION_ERROR", errors));
    }

    @ExceptionHandler(KafkaPublishException.class)
    public ResponseEntity<ApiResponse<Void>> handleKafkaPublishFailure(KafkaPublishException ex) {
        log.error("Kafka publish failed for event {}: {}", ex.getEvent().getId(), ex.getMessage());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("KAFKA_PUBLISH_FAILURE",
                        "Event accepted but Kafka publish failed. Source system should retry."));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleGeneric(Exception ex) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error("INTERNAL_ERROR", "An unexpected error occurred"));
    }
}
