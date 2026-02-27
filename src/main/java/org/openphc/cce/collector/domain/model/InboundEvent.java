package org.openphc.cce.collector.domain.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.openphc.cce.collector.domain.model.enums.FailureStage;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Single table design — audit trail, dedup source, rejection tracking, and Kafka source.
 * Every HTTP request is persisted as-is before processing. Rejected events are recorded
 * directly on this table — there is no separate dead letter table.
 */
@Entity
@Table(name = "inbound_event")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InboundEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "cloudevents_id", nullable = false)
    private String cloudeventsId;

    @Column(nullable = false)
    private String source;

    @Column(nullable = false)
    private String type;

    @Column(name = "spec_version", nullable = false)
    @Builder.Default
    private String specVersion = "1.0";

    private String subject;

    @Column(name = "event_time")
    private OffsetDateTime eventTime;

    @Column(name = "data_content_type")
    @Builder.Default
    private String dataContentType = "application/fhir+json";

    @Column(name = "facility_id")
    private String facilityId;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "source_event_id")
    private String sourceEventId;

    @Column(name = "raw_payload", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> rawPayload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    @Builder.Default
    private InboundStatus status = InboundStatus.RECEIVED;

    @Enumerated(EnumType.STRING)
    @Column(name = "rejection_reason")
    private RejectionReason rejectionReason;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_stage")
    private FailureStage failureStage;

    @Column(name = "error_details", columnDefinition = "TEXT")
    private String errorDetails;

    @Column(nullable = false)
    @Builder.Default
    private boolean resolved = false;

    @Column(name = "resolved_at")
    private OffsetDateTime resolvedAt;

    @Column(name = "received_at", nullable = false)
    @Builder.Default
    private OffsetDateTime receivedAt = OffsetDateTime.now();
}
