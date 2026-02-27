package org.openphc.cce.collector.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.*;
import org.openphc.cce.collector.api.exception.CloudEventValidationException;
import org.openphc.cce.collector.api.exception.FhirValidationException;
import org.openphc.cce.collector.api.exception.InvalidEventTypeException;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.openphc.cce.collector.api.exception.UnsupportedContentTypeException;
import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.model.enums.FailureStage;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventRepository;
import org.openphc.cce.collector.kafka.InboundEventProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Main orchestrator: validate → deduplicate → persist → validate type → enrich → validate payload → publish.
 * Implements the single-table, synchronous-publish event ingestion flow.
 */
@Service
@Slf4j
public class EventIngestionService {

    private final CloudEventValidator cloudEventValidator;
    private final EventTypeValidator eventTypeValidator;
    private final EventDefaultsEnricher eventDefaultsEnricher;
    private final FhirPayloadValidator fhirPayloadValidator;
    private final DeduplicationService deduplicationService;
    private final RejectionService rejectionService;
    private final InboundEventProducer inboundEventProducer;
    private final InboundEventRepository inboundEventRepository;
    private final String inboundTopic;

    // Metrics
    private final Timer ingestionTimer;
    private final MeterRegistry meterRegistry;

    public EventIngestionService(
            CloudEventValidator cloudEventValidator,
            EventTypeValidator eventTypeValidator,
            EventDefaultsEnricher eventDefaultsEnricher,
            FhirPayloadValidator fhirPayloadValidator,
            DeduplicationService deduplicationService,
            RejectionService rejectionService,
            InboundEventProducer inboundEventProducer,
            InboundEventRepository inboundEventRepository,
            @Value("${cce.kafka.topics.inbound}") String inboundTopic,
            MeterRegistry meterRegistry) {
        this.cloudEventValidator = cloudEventValidator;
        this.eventTypeValidator = eventTypeValidator;
        this.eventDefaultsEnricher = eventDefaultsEnricher;
        this.fhirPayloadValidator = fhirPayloadValidator;
        this.deduplicationService = deduplicationService;
        this.rejectionService = rejectionService;
        this.inboundEventProducer = inboundEventProducer;
        this.inboundEventRepository = inboundEventRepository;
        this.inboundTopic = inboundTopic;
        this.meterRegistry = meterRegistry;
        this.ingestionTimer = Timer.builder("cce.collector.ingestion.duration")
                .description("End-to-end event ingestion latency")
                .register(meterRegistry);
    }

    /**
     * Ingest a single clinical event — the primary entry point.
     */
    public EventIngestionResponse ingest(EventIngestionRequest request) {
        return ingestionTimer.record(() -> doIngest(request));
    }

    /**
     * Core ingestion logic implementing the 9-step flow:
     * 1. Receive → 2. Validate envelope → 3. Dedup → 4. Persist → 5. Validate type →
     * 6. Enrich defaults → 7. Validate payload → 8. Accept → 9. Kafka publish
     */
    private EventIngestionResponse doIngest(EventIngestionRequest request) {
        OffsetDateTime receivedAt = OffsetDateTime.now(ZoneOffset.UTC);

        // Step 2: CloudEvents envelope validation
        try {
            cloudEventValidator.validate(request);
        } catch (CloudEventValidationException e) {
            recordMetric(request.getSource(), "rejected");
            throw e;
        }

        // Step 3: Deduplication check (before DB persist to avoid constraint violations)
        if (deduplicationService.isDuplicate(request.getSource(), request.getId())) {
            recordMetric(request.getSource(), "duplicate");
            return buildDuplicateResponse(request, receivedAt);
        }

        // Step 4: Persist raw inbound event (status = RECEIVED, first write — audit trail)
        InboundEvent inboundEvent = persistInboundEvent(request, receivedAt);

        // Step 5: Event type validation (strict — no normalization)
        if (!eventTypeValidator.isValid(request.getType())) {
            rejectionService.reject(inboundEvent, RejectionReason.INVALID_EVENT_TYPE,
                    FailureStage.VALIDATION,
                    "Event type '" + request.getType() + "' does not match required pattern org.openphc.cce.<resource>");
            recordMetric(request.getSource(), "rejected");
            throw new InvalidEventTypeException(request.getType());
        }

        // Step 6: Default enrichment
        String correlationId = eventDefaultsEnricher.ensureCorrelationId(request.getCorrelationid());
        OffsetDateTime eventTime = eventDefaultsEnricher.ensureEventTime(request.getTime());
        String dataContentType = eventDefaultsEnricher.ensureDataContentType(request.getDatacontenttype());

        // Update enriched fields on the inbound event
        inboundEvent.setCorrelationId(correlationId);
        inboundEvent.setEventTime(eventTime);
        inboundEvent.setDataContentType(dataContentType);

        // Step 7: Payload validation (branched by datacontenttype)
        FhirPayloadValidator.PayloadValidationResult payloadResult =
                fhirPayloadValidator.validate(request, dataContentType);
        if (!payloadResult.isValid()) {
            rejectionService.reject(inboundEvent, payloadResult.getRejectionReason(),
                    FailureStage.VALIDATION, String.join("; ", payloadResult.getErrors()));
            recordMetric(request.getSource(), "rejected");

            // Throw the correct exception based on rejection reason
            if (payloadResult.getRejectionReason() == RejectionReason.UNSUPPORTED_CONTENT_TYPE) {
                throw new UnsupportedContentTypeException(
                        dataContentType, payloadResult.getMessage(), payloadResult.getErrors());
            }
            throw new FhirValidationException(payloadResult.getMessage(), payloadResult.getErrors());
        }

        // Step 8: Update inbound event status to ACCEPTED
        inboundEvent.setStatus(InboundStatus.ACCEPTED);
        inboundEventRepository.save(inboundEvent);

        // Step 9: Synchronous Kafka publish
        CloudEventMessage message = buildCloudEventMessage(inboundEvent, request);
        try {
            inboundEventProducer.publish(message);
        } catch (Exception e) {
            log.error("Kafka publish failed for event id={}: {}", request.getId(), e.getMessage());
            rejectionService.reject(inboundEvent, RejectionReason.KAFKA_PUBLISH_FAILURE,
                    FailureStage.KAFKA_PUBLISH, e.getMessage());
            recordMetric(request.getSource(), "kafka_failure");
            throw new KafkaPublishException(message, e);
        }

        recordMetric(request.getSource(), "accepted");

        return EventIngestionResponse.builder()
                .eventId(request.getId())
                .status("accepted")
                .correlationId(correlationId)
                .publishedTopic(inboundTopic)
                .receivedAt(receivedAt)
                .build();
    }

    /**
     * Persist the raw inbound event (audit trail, first write).
     */
    @Transactional
    protected InboundEvent persistInboundEvent(EventIngestionRequest request, OffsetDateTime receivedAt) {
        InboundEvent inboundEvent = InboundEvent.builder()
                .cloudeventsId(request.getId())
                .source(request.getSource())
                .type(request.getType())
                .specVersion(request.getSpecversion())
                .subject(request.getSubject())
                .eventTime(request.getTime() != null && !request.getTime().isBlank()
                        ? OffsetDateTime.parse(request.getTime()) : null)
                .dataContentType(request.getDatacontenttype())
                .facilityId(request.getFacilityid())
                .correlationId(request.getCorrelationid())
                .sourceEventId(request.getSourceeventid())
                .rawPayload(request.toRawPayload())
                .status(InboundStatus.RECEIVED)
                .receivedAt(receivedAt)
                .build();

        return inboundEventRepository.save(inboundEvent);
    }

    /**
     * Build a CloudEventMessage from inbound event and request data for Kafka publishing.
     */
    private CloudEventMessage buildCloudEventMessage(InboundEvent inboundEvent, EventIngestionRequest request) {
        return CloudEventMessage.builder()
                .id(inboundEvent.getCloudeventsId())
                .source(inboundEvent.getSource())
                .type(inboundEvent.getType())
                .specversion("1.0")
                .subject(inboundEvent.getSubject())
                .time(inboundEvent.getEventTime())
                .datacontenttype(inboundEvent.getDataContentType())
                .correlationid(inboundEvent.getCorrelationId())
                .sourceeventid(inboundEvent.getSourceEventId())
                .protocolinstanceid(request.getProtocolinstanceid())
                .protocoldefinitionid(request.getProtocoldefinitionid())
                .actionid(request.getActionid())
                .facilityid(inboundEvent.getFacilityId())
                .data(request.getData())
                .build();
    }

    private EventIngestionResponse buildDuplicateResponse(EventIngestionRequest request, OffsetDateTime receivedAt) {
        return EventIngestionResponse.builder()
                .eventId(request.getId())
                .status("duplicate")
                .correlationId(request.getCorrelationid())
                .receivedAt(receivedAt)
                .build();
    }

    private void recordMetric(String source, String status) {
        Counter.builder("cce.collector.events.received")
                .tag("source", source != null ? source : "unknown")
                .tag("status", status)
                .register(meterRegistry)
                .increment();
    }
}
