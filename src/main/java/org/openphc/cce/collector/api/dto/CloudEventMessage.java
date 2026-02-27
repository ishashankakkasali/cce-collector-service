package org.openphc.cce.collector.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * CloudEventMessage — the Kafka message published to cce.events.inbound.
 * Field names use lowercase per the CloudEvents spec — no field name translation is performed.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CloudEventMessage {

    private String id;
    private String source;
    private String type;
    private String specversion;
    private String subject;
    private OffsetDateTime time;
    private String datacontenttype;
    private String correlationid;
    private String sourceeventid;
    private String protocolinstanceid;
    private String protocoldefinitionid;
    private String actionid;
    private String facilityid;
    private Map<String, Object> data;
}
