package org.openphc.cce.collector.domain.model.enums;

/**
 * Reason an event was rejected.
 * Rejection details are tracked on the inbound_event table (single-table design).
 */
public enum RejectionReason {
    INVALID_ENVELOPE,
    INVALID_EVENT_TYPE,
    INVALID_FHIR,
    INVALID_JSON,
    UNSUPPORTED_CONTENT_TYPE,
    DUPLICATE,
    MISSING_SUBJECT,
    PAYLOAD_TOO_LARGE,
    DESERIALIZATION_ERROR,
    KAFKA_PUBLISH_FAILURE
}
