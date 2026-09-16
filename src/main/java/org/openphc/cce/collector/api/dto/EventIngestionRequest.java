package org.openphc.cce.collector.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Inbound DTO for CloudEvents v1.0 event ingestion.
 *
 * <p>All field names use <strong>lowercase</strong> per the CloudEvents
 * specification — no camelCase translation. Multi-word CloudEvents
 * attribute names are concatenated lowercase (e.g. {@code specversion},
 * {@code datacontenttype}, {@code correlationid}).</p>
 *
 * <p>Bean Validation annotations enforce presence of required fields.
 * Detailed business-rule validation (e.g. specversion must be "1.0",
 * id max 50 chars) is handled by {@code CloudEventValidator} (C5).</p>
 *
 * <p>Mutable class — {@code EventDefaultsEnricher} (C8) may set
 * {@code correlationid} and {@code time} when absent.</p>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventIngestionRequest {

    // ─── CloudEvents required attributes ───────────────────────────

    @NotBlank(message = "specversion is required")
    private String specversion;

    @NotBlank(message = "id is required")
    private String id;

    @NotBlank(message = "source is required")
    private String source;

    @NotBlank(message = "type is required")
    private String type;

    @NotBlank(message = "subject is required")
    private String subject;

    // ─── CloudEvents recommended attributes ────────────────────────

    /** ISO-8601 datetime string; filled by server if absent. */
    private String time;

    /** MIME type of data payload. Must be {@code application/fhir+json} or {@code application/json}. */
    @NotBlank(message = "datacontenttype is required")
    private String datacontenttype;

    // ─── CloudEvents data ──────────────────────────────────────────

    @NotNull(message = "data is required")
    private JsonNode data;

    // ─── CCE extension attributes (lowercase per CloudEvents spec) ─

    private String facilityid;
    private String facilityname;
    private String correlationid;
    private String sourceeventid;
    private String protocolinstanceid;
    private String protocoldefinitionid;
    private String actionid;
}
