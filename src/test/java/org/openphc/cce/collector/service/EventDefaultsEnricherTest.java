package org.openphc.cce.collector.service;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventDefaultsEnricher — correlation ID generation, time fill, content type defaults.
 */
class EventDefaultsEnricherTest {

    private final EventDefaultsEnricher enricher = new EventDefaultsEnricher();

    // --- Correlation ID ---

    @Test
    void shouldReturnExistingCorrelationId() {
        assertEquals("existing-corr-id", enricher.ensureCorrelationId("existing-corr-id"));
    }

    @Test
    void shouldGenerateCorrelationIdWhenNull() {
        String generated = enricher.ensureCorrelationId(null);
        assertNotNull(generated);
        assertTrue(generated.startsWith("corr-"), "Generated ID should start with 'corr-'");
    }

    @Test
    void shouldGenerateCorrelationIdWhenBlank() {
        String generated = enricher.ensureCorrelationId("   ");
        assertNotNull(generated);
        assertTrue(generated.startsWith("corr-"), "Generated ID should start with 'corr-'");
    }

    @Test
    void shouldGenerateCorrelationIdWhenEmpty() {
        String generated = enricher.ensureCorrelationId("");
        assertNotNull(generated);
        assertTrue(generated.startsWith("corr-"));
    }

    // --- Event Time ---

    @Test
    void shouldParseValidEventTime() {
        String rawTime = "2026-02-27T10:00:00Z";
        OffsetDateTime result = enricher.ensureEventTime(rawTime);
        assertNotNull(result);
        assertEquals(2026, result.getYear());
        assertEquals(2, result.getMonthValue());
        assertEquals(27, result.getDayOfMonth());
    }

    @Test
    void shouldReturnServerTimeWhenNull() {
        OffsetDateTime result = enricher.ensureEventTime(null);
        assertNotNull(result);
        // Should be approximately now
        assertTrue(OffsetDateTime.now().minusMinutes(1).isBefore(result));
    }

    @Test
    void shouldReturnServerTimeWhenBlank() {
        OffsetDateTime result = enricher.ensureEventTime("  ");
        assertNotNull(result);
    }

    @Test
    void shouldReturnServerTimeWhenInvalid() {
        OffsetDateTime result = enricher.ensureEventTime("not-a-date");
        assertNotNull(result);
        // Should be approximately now (fallback)
        assertTrue(OffsetDateTime.now().minusMinutes(1).isBefore(result));
    }

    // --- Data Content Type ---

    @Test
    void shouldReturnExistingDataContentType() {
        assertEquals("application/json", enricher.ensureDataContentType("application/json"));
    }

    @Test
    void shouldDefaultToFhirJsonWhenNull() {
        assertEquals("application/fhir+json", enricher.ensureDataContentType(null));
    }

    @Test
    void shouldDefaultToFhirJsonWhenBlank() {
        assertEquals("application/fhir+json", enricher.ensureDataContentType("   "));
    }

    @Test
    void shouldDefaultToFhirJsonWhenEmpty() {
        assertEquals("application/fhir+json", enricher.ensureDataContentType(""));
    }
}
