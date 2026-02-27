package org.openphc.cce.collector.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * DTO for rejected event responses (from inbound_event table).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RejectedEventDto {

    private UUID id;
    private String cloudeventsId;
    private String source;
    private String type;
    private String subject;
    private Map<String, Object> rawPayload;
    private String rejectionReason;
    private String failureStage;
    private String errorDetails;
    private String correlationId;
    private String facilityId;
    private OffsetDateTime receivedAt;
    private boolean resolved;
    private OffsetDateTime resolvedAt;
}
