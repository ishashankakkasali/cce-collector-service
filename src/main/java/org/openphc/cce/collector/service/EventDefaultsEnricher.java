package org.openphc.cce.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Enriches events with server-generated defaults when optional fields are missing.
 * This is NOT normalization — the event type and other source-provided fields are never modified.
 */
@Component
@Slf4j
public class EventDefaultsEnricher {

    private static final String DEFAULT_CONTENT_TYPE = "application/fhir+json";

    /**
     * Ensure correlation ID is present — generate one if absent.
     */
    public String ensureCorrelationId(String existing) {
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        String generated = "corr-" + UUID.randomUUID();
        log.debug("Generated correlation ID: {}", generated);
        return generated;
    }

    /**
     * Ensure event time is present — fill with server received timestamp if absent.
     */
    public OffsetDateTime ensureEventTime(String rawTime) {
        if (rawTime != null && !rawTime.isBlank()) {
            try {
                return OffsetDateTime.parse(rawTime);
            } catch (Exception e) {
                log.warn("Failed to parse event time '{}', using server time", rawTime);
            }
        }
        return OffsetDateTime.now(ZoneOffset.UTC);
    }

    /**
     * Ensure datacontenttype is present — default to application/fhir+json if absent.
     */
    public String ensureDataContentType(String existing) {
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        log.debug("datacontenttype absent, defaulting to {}", DEFAULT_CONTENT_TYPE);
        return DEFAULT_CONTENT_TYPE;
    }
}
