package org.openphc.cce.collector.domain.repository;

import org.openphc.cce.collector.domain.model.InboundEvent;
import org.openphc.cce.collector.domain.model.enums.InboundStatus;
import org.openphc.cce.collector.domain.model.enums.RejectionReason;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface InboundEventRepository extends JpaRepository<InboundEvent, UUID> {

    Optional<InboundEvent> findByCloudeventsIdAndSource(String cloudeventsId, String source);

    boolean existsByCloudeventsIdAndSource(String cloudeventsId, String source);

    boolean existsByCloudeventsIdAndSourceAndReceivedAtAfter(String cloudeventsId, String source, OffsetDateTime since);

    long countByStatus(InboundStatus status);

    long countByStatusAndResolved(InboundStatus status, boolean resolved);

    Page<InboundEvent> findByStatusAndResolvedFalse(InboundStatus status, Pageable pageable);

    Page<InboundEvent> findByStatusAndRejectionReason(InboundStatus status, RejectionReason reason, Pageable pageable);

    Page<InboundEvent> findByStatusAndSource(InboundStatus status, String source, Pageable pageable);

    Page<InboundEvent> findByStatusAndResolved(InboundStatus status, boolean resolved, Pageable pageable);
}
