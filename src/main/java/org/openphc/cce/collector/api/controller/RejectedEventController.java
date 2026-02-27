package org.openphc.cce.collector.api.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.api.dto.ApiResponse;
import org.openphc.cce.collector.api.dto.RejectedEventDto;
import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventRepository;
import org.openphc.cce.collector.service.RejectionService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * Rejected event management controller — view, query, and retry rejected events.
 * Reads from the inbound_event table where status = REJECTED.
 */
@RestController
@RequestMapping("/v1/events/rejected")
@RequiredArgsConstructor
@Slf4j
public class RejectedEventController {

    private final InboundEventRepository inboundEventRepository;
    private final RejectionService rejectionService;

    @GetMapping
    public ResponseEntity<ApiResponse<Page<RejectedEventDto>>> listRejectedEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String reason,
            @RequestParam(required = false) String source,
            @RequestParam(required = false) Boolean resolved) {

        PageRequest pageRequest = PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "receivedAt"));

        Page<InboundEvent> events;
        if (reason != null && !reason.isBlank()) {
            events = inboundEventRepository.findByStatusAndRejectionReason(
                    InboundStatus.REJECTED, RejectionReason.valueOf(reason.toUpperCase()), pageRequest);
        } else if (source != null && !source.isBlank()) {
            events = inboundEventRepository.findByStatusAndSource(InboundStatus.REJECTED, source, pageRequest);
        } else if (resolved != null) {
            events = inboundEventRepository.findByStatusAndResolved(InboundStatus.REJECTED, resolved, pageRequest);
        } else {
            events = inboundEventRepository.findByStatusAndResolvedFalse(InboundStatus.REJECTED, pageRequest);
        }

        Page<RejectedEventDto> dtoPage = events.map(this::toDto);
        return ResponseEntity.ok(ApiResponse.success(dtoPage));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ApiResponse<RejectedEventDto>> getRejectedEvent(@PathVariable UUID id) {
        return inboundEventRepository.findById(id)
                .filter(e -> e.getStatus() == InboundStatus.REJECTED)
                .map(event -> ResponseEntity.ok(ApiResponse.success(toDto(event))))
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<ApiResponse<String>> retryRejectedEvent(@PathVariable UUID id) {
        return rejectionService.resolve(id)
                .map(event -> {
                    log.info("Rejected event {} marked as resolved for retry", id);
                    return ResponseEntity.ok(ApiResponse.success("Rejected event marked for retry"));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    private RejectedEventDto toDto(InboundEvent event) {
        return RejectedEventDto.builder()
                .id(event.getId())
                .cloudeventsId(event.getCloudeventsId())
                .source(event.getSource())
                .type(event.getType())
                .subject(event.getSubject())
                .rawPayload(event.getRawPayload())
                .rejectionReason(event.getRejectionReason() != null ? event.getRejectionReason().name() : null)
                .failureStage(event.getFailureStage() != null ? event.getFailureStage().name() : null)
                .errorDetails(event.getErrorDetails())
                .correlationId(event.getCorrelationId())
                .facilityId(event.getFacilityId())
                .receivedAt(event.getReceivedAt())
                .resolved(event.isResolved())
                .resolvedAt(event.getResolvedAt())
                .build();
    }
}
