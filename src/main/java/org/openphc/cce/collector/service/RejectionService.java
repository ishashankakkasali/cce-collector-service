package org.openphc.cce.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.model.enums.FailureStage;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.openphc.cce.collector.domain.repository.InboundEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Records rejection details directly on the inbound_event table (single-table design).
 * No separate dead letter table — rejection tracking columns are on inbound_event.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RejectionService {

    private final InboundEventRepository inboundEventRepository;

    /**
     * Mark an inbound event as rejected with reason, stage, and error details.
     */
    @Transactional
    public void reject(InboundEvent event, RejectionReason reason, FailureStage stage, String errorDetails) {
        event.setStatus(InboundStatus.REJECTED);
        event.setRejectionReason(reason);
        event.setFailureStage(stage);
        event.setErrorDetails(errorDetails);
        inboundEventRepository.save(event);
        log.error("Rejected event: id={}, source={}, reason={}, stage={}, details={}",
                event.getCloudeventsId(), event.getSource(), reason, stage, errorDetails);
    }

    /**
     * Mark a rejected event as resolved (for retry management).
     */
    @Transactional
    public Optional<InboundEvent> resolve(UUID id) {
        return inboundEventRepository.findById(id).map(event -> {
            event.setResolved(true);
            event.setResolvedAt(OffsetDateTime.now());
            return inboundEventRepository.save(event);
        });
    }

    /**
     * Count unresolved rejected events.
     */
    public long countUnresolved() {
        return inboundEventRepository.countByStatusAndResolved(InboundStatus.REJECTED, false);
    }
}
