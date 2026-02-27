package org.openphc.cce.collector.service;

import org.junit.jupiter.api.Test;
import org.openphc.cce.collector.api.dto.EventIngestionRequest;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CloudEventValidator — validates CloudEvents v1.0 envelope.
 * Validates that all errors are aggregated (not thrown on first failure).
 */
class CloudEventValidatorTest {

    private final CloudEventValidator validator = new CloudEventValidator();

    @Test
    void shouldAcceptValidCloudEvent() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        assertDoesNotThrow(() -> validator.validate(request));
    }

    @Test
    void shouldRejectMissingSpecVersion() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("specversion")));
    }

    @Test
    void shouldRejectWrongSpecVersion() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("2.0")
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("specversion")));
    }

    @Test
    void shouldRejectMissingId() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'id'")));
    }

    @Test
    void shouldRejectMissingSource() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'source'")));
    }

    @Test
    void shouldRejectMissingType() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("rhie-mediator")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'type'")));
    }

    @Test
    void shouldRejectMissingSubject() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'subject'")));
    }

    @Test
    void shouldRejectMissingData() {
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .id("evt-001")
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'data'")));
    }

    @Test
    void shouldRejectIdExceedingMaxLength() {
        String longId = "x".repeat(257);
        EventIngestionRequest request = EventIngestionRequest.builder()
                .specversion("1.0")
                .id(longId)
                .source("rhie-mediator")
                .type("org.openphc.cce.encounter")
                .subject("260225-0002-5501")
                .data(Map.of("resourceType", "Encounter"))
                .build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("max length")));
    }

    @Test
    void shouldAggregateAllErrors() {
        // Request missing specversion, id, source, type, subject, and data — all errors should be reported
        EventIngestionRequest request = EventIngestionRequest.builder().build();

        CloudEventValidationException ex = assertThrows(
                CloudEventValidationException.class,
                () -> validator.validate(request));

        // Should have at least 6 errors (one for each required field)
        assertTrue(ex.getErrors().size() >= 6,
                "Expected at least 6 errors but got " + ex.getErrors().size() + ": " + ex.getErrors());
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'specversion'")));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'id'")));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'source'")));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'type'")));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'subject'")));
        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("'data'")));
    }
}
