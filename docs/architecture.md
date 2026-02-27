# CCE Collector Service — Architecture

## 1. Purpose

The Collector Service is the **single point of entry** for all clinical events into the Care Coordination Engine (CCE) platform. No external system publishes directly to Kafka — every event flows through the Collector.

It receives clinical events from external EHR/RHIE systems (via openHIM mediators or direct integrations), validates them as CloudEvents v1.0 envelopes with FHIR R4 payloads, and publishes them to Kafka for downstream processing by the Compliance Service. Event type normalization is the responsibility of the emitter adaptor (openHIM mediator layer); the Collector enforces the expected `org.openphc.cce.<resource>` format and rejects non-conforming events.

## 2. Responsibilities

| # | Responsibility | Description |
|---|---------------|-------------|
| 1 | **Receive clinical events** | REST API ingestion from openHIM RHIE mediators, EMR direct push, CHW apps |
| 2 | **Validate CloudEvents envelope** | Mandatory fields (`specversion`, `id`, `source`, `type`, `subject`), extensions, structure |
| 3 | **Validate FHIR R4 payloads** | Structural validation of `data` when `datacontenttype` = `application/fhir+json` |
| 4 | **Enforce event type contract** | Reject events with `type` not matching `org.openphc.cce.<resource>` pattern — normalization is the emitter adaptor's responsibility |
| 5 | **Apply server-side defaults** | Generate `correlationid` (UUID with `corr-` prefix) if absent; fill `time` with server `received_at` if absent |
| 6 | **Deduplicate inbound events** | Reject/mark duplicates using `(id, source)` compound key via PostgreSQL with configurable lookback window |
| 7 | **Publish to Kafka** | Topic `cce.events.inbound` with `subject` (patient_id) as partition key |
| 8 | **Record rejection details** | Persist failure stage, error details, and resolution tracking on `inbound_event` for rejected events |
| 9 | **Health/readiness endpoints** | For Kubernetes orchestration |

### Explicit Exclusions

- Protocol matching, step completion, or compliance tracking → **Compliance Service**
- Time-based state transitions → **Scheduler Service**
- OAuth token management, routing, rate limiting → **Gateway Service**
- Analytics or reporting → **Analytics Service**
- Event type normalization (emitter adaptor responsibility — Collector only validates the format)
- FHIR profile conformance validation (structural parse only)
- Event transformation or enrichment beyond server-side defaults (`correlationid`, `time`)
- Event routing to multiple topics

## 3. System Context

```
┌─────────────────────────────────────────────────────────────┐
│                    External Systems                          │
│  eBUZIMA EMR  │  SmartCare  │  CHW App  │  Lab Systems      │
└──────┬────────┴──────┬──────┴─────┬─────┴──────┬────────────┘
       │               │            │            │
       ▼               ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│               openHIM / RHIE Mediator Layer                  │
│         (routes, transforms, adds correlation IDs)           │
└──────────────────────────┬──────────────────────────────────┘
                           │  HTTP POST (CloudEvents)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              ★ CCE Collector Service ★                        │
│    Validate → Deduplicate → Publish to Kafka                 │
└──────────────────────────┬──────────────────────────────────┘
                           │  Kafka: cce.events.inbound
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              CCE Compliance Service                           │
│    Match → Enroll → Complete Steps → Detect Deviations       │
└─────────────────────────────────────────────────────────────┘
```

## 4. Technology Stack

| Concern | Technology | Version |
|---------|------------|---------|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 3.4.x |
| Build tool | Maven | 3.9+ |
| Database | PostgreSQL | 16+ |
| Message broker | Apache Kafka | 3.7+ (KRaft mode) |
| FHIR library | HAPI FHIR | 7.4.0 |
| DB access | Spring Data JPA + Hibernate | (Spring Boot managed) |
| DB migration | Flyway | (Spring Boot managed) |
| Connection pool | HikariCP | (Spring Boot default) |
| Observability | Micrometer + Prometheus | (Spring Boot managed) |
| Testing | JUnit 5, Testcontainers, MockMvc | |

## 5. Package Structure

```
org.openphc.cce.collector/
├── CollectorServiceApplication.java       # Spring Boot entry point
├── config/                                # Spring configuration beans
│   ├── KafkaProducerConfig.java           #   Kafka producer factory, templates, topic creation
│   ├── FhirConfig.java                    #   FhirContext.forR4() singleton bean
│   ├── JpaConfig.java                     #   Enables JPA repositories & transactions
│   ├── SecurityConfig.java                #   Stateless security, CSRF disabled
│   └── WebConfig.java                     #   CORS configuration
├── domain/
│   ├── model/                             # JPA entities
│   │   ├── InboundEvent.java              #   Raw inbound request record (audit/dedup/rejection tracking)
│   │   └── enums/
│   │       ├── InboundStatus.java         #   RECEIVED, ACCEPTED, REJECTED, DUPLICATE
│   │       ├── RejectionReason.java       #   Validation/processing failure reasons
│   │       └── FailureStage.java          #   VALIDATION, PROCESSING, KAFKA_PUBLISH
│   └── repository/                        # Spring Data JPA repositories
│       └── InboundEventRepository.java
├── service/                               # Core business logic
│   ├── EventIngestionService.java         #   Main orchestrator: validate → persist → publish
│   ├── CloudEventValidator.java           #   CloudEvents v1.0 envelope validation
│   ├── FhirPayloadValidator.java          #   FHIR R4 structural validation via HAPI
│   ├── EventTypeValidator.java             #   Validates type matches org.openphc.cce.<resource> pattern
│   ├── EventDefaultsEnricher.java         #   Generates correlationId if absent, fills time if absent
│   ├── DeduplicationService.java          #   DB dedup with configurable lookback window
│   └── RejectionService.java              #   Updates inbound_event with rejection details
├── kafka/
│   └── InboundEventProducer.java          # Kafka publish to cce.events.inbound
├── api/
│   ├── controller/
│   │   ├── EventIngestionController.java  #   POST /v1/events
│   │   └── RejectedEventController.java   #   Rejected event queries and resolution
│   ├── dto/                               # Request/response data transfer objects
│   │   ├── ApiResponse.java               #   { "data": ... } envelope
│   │   ├── ApiError.java                  #   { "error": { "code", "message" } }
│   │   ├── EventIngestionRequest.java     #   CloudEvents envelope (inbound DTO)
│   │   ├── EventIngestionResponse.java    #   Accepted/rejected receipt
│   │   ├── CloudEventMessage.java         #   Kafka message DTO (CloudEvents spec field names)
│   │   └── RejectedEventDto.java          #   Rejected event response DTO
│   └── exception/
│       ├── GlobalExceptionHandler.java    #   @ControllerAdvice centralized error handling
│       ├── CloudEventValidationException.java
│       ├── FhirValidationException.java
│       ├── KafkaPublishException.java
│       └── DuplicateEventException.java
└── fhir/
    ├── FhirResourceParser.java            # HAPI FHIR parse + type detection
    └── FhirResourceValidator.java         # Structural validation + subject cross-check
```

## 6. Core Processing Algorithm

### Single Event Ingestion Flow

```
 1. Receive HTTP POST → parse request body
 2. CloudEvents Envelope Validation
    a. Required fields: specversion, id, source, type, subject, data
    b. specversion must be "1.0"
    c. subject must be non-empty (patient UPID required by CCE)
    d. If validation fails → 400 + update inbound_event status = 'REJECTED' with rejection details
 3. Deduplication Check
    a. Query PostgreSQL: check if (source, cloudevents_id) exists within lookback window
       - If exists → update status = 'DUPLICATE', return 200 (idempotent)
    b. If not found → proceed (DB unique constraint is authoritative)
 4. Persist to inbound_event (status = 'RECEIVED', raw_payload = original body)
 5. Event Type Validation
    a. Validate type matches org.openphc.cce.<resource> pattern
    b. If invalid → status = 'REJECTED', rejection_reason = INVALID_EVENT_TYPE, return 400
    (Event type normalization is the emitter adaptor's responsibility)
 6. Apply Server-Side Defaults
    a. Generate correlationid if absent (UUID with "corr-" prefix)
    b. Fill time with server received_at if absent
 7. Payload Validation
    a. If datacontenttype = application/fhir+json (or absent — default):
       i.   Parse data via HAPI FHIR
       ii.  Validate resourceType is present and parseable
       iii. Cross-check subject reference (warning only)
       iv.  If invalid → status = 'REJECTED', rejection_reason = INVALID_FHIR, return 422
    b. If datacontenttype = application/json (non-FHIR):
       i.   Validate data is valid JSON
       ii.  If invalid → status = 'REJECTED', rejection_reason = INVALID_JSON, return 422
       iii. No FHIR-specific validation is performed
    c. Other datacontenttype values → status = 'REJECTED', rejection_reason = UNSUPPORTED_CONTENT_TYPE, return 400
 8. Update inbound_event.status = 'ACCEPTED'
 9. Publish to Kafka synchronously
    a. Key = subject (patient_id) — per-patient ordering
    b. On success: return HTTP 202 Accepted with ingestion receipt
    c. On failure: update inbound_event status = 'REJECTED', rejection_reason = KAFKA_PUBLISH_FAILURE, return HTTP 500
       Source system (openHIM) will retry based on its retry policy
```

## 7. Database Schema

One table owned by this service, managed by Flyway:

| Table | Purpose | Partitioned |
|-------|---------|-------------|
| `inbound_event` | Raw request audit log + rejection tracking; primary dedup via `UNIQUE(cloudevents_id, source)` | No |

### Entity

```
inbound_event  (single table — audit, dedup, rejection tracking)
```

> **Note:** Rejected events are tracked directly on `inbound_event` via `status`, `rejection_reason`, `failure_stage`, `error_details`, and `resolved`/`resolved_at` columns. 

### Deduplication Constraints

| Constraint | Table | Purpose |
|-----------|-------|---------|
| `UNIQUE(cloudevents_id, source)` | `inbound_event` | Primary dedup |


## 8. Deduplication Strategy

PostgreSQL-based deduplication with a configurable lookback window (default: 30 days) + unique constraints (permanent).

### Lookback Query

On event arrival, the service queries `inbound_event` for records matching `(source, cloudevents_id)` within the configured lookback window. This limits the query scope instead of scanning the entire database.

### PostgreSQL Unique Constraint (Authoritative)

The unique constraint on `inbound_event` serves as the permanent deduplication layer.

### Idempotency Contract

- Same event submitted twice → **200 OK** with `status: "duplicate"` (not 409)
- Event is **not** re-published to Kafka on duplicate
- Standard idempotent POST pattern used by openHIM mediators

## 9. Kafka Integration

### Topics Produced

| Topic | Key | Purpose |
|-------|-----|---------|
| `cce.events.inbound` | `subject` (patient UPID) | Validated events for Compliance Service |
| `cce.deadletter` | `correlationId` | Rejected/failed events for monitoring (optional) |

### Producer Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| `acks` | `all` | Wait for all in-sync replicas |
| `retries` | `3` | Retry on transient failures |
| `enable.idempotence` | `true` | Exactly-once within a partition |
| `linger.ms` | `5` | Small batching window |
| Partitions | 12 | Supports 12 parallel consumers |

### Kafka Publish

Kafka publish is **synchronous** within the HTTP request. On failure, the Collector returns HTTP 500 and marks the event as `REJECTED` with `KAFKA_PUBLISH_FAILURE`. The source system (openHIM) is expected to retry.

> **No outbox pattern.** There is no scheduled retry for failed Kafka publishes. The Collector relies on the source system's retry behaviour.

## 10. Payload Handling

The Collector supports two `datacontenttype` values, determining the level of payload validation applied:

### `application/fhir+json` (default)

FHIR R4 structural validation via HAPI FHIR.

| Check | Level | Action on Failure |
|-------|-------|-------------------|
| `data` parses as valid JSON | Required | Reject (`INVALID_FHIR`) |
| `data.resourceType` present and non-empty | Required | Reject (`INVALID_FHIR`) |
| HAPI FHIR can parse into `IBaseResource` | Required | Reject (`INVALID_FHIR`) |
| `data.subject.reference` matches `subject` | Warning | Accept, log warning |

### `application/json` (non-FHIR)

Non-FHIR JSON payloads are also supported per the CCE solution design. Only basic JSON validity is checked.

| Check | Level | Action on Failure |
|-------|-------|-------------------|
| `data` parses as valid JSON | Required | Reject (`INVALID_JSON`) |
| `data` is a non-empty JSON object | Required | Reject (`INVALID_JSON`) |

No FHIR-specific validation (resourceType, HAPI parsing, subject cross-check) is performed for non-FHIR payloads.

### Unsupported content types

Any `datacontenttype` other than `application/fhir+json` or `application/json` is rejected with `UNSUPPORTED_CONTENT_TYPE`.

### Supported Resource Types

| Resource Type | Event Type |
|---------------|------------|
| `Encounter` | `org.openphc.cce.encounter` |
| `Observation` | `org.openphc.cce.observation` |
| `Condition` | `org.openphc.cce.condition` |
| `MedicationRequest` | `org.openphc.cce.medicationrequest` |
| `MedicationDispense` | `org.openphc.cce.medicationdispense` |
| `ServiceRequest` | `org.openphc.cce.servicerequest` |
| `Procedure` | `org.openphc.cce.procedure` |
| `EpisodeOfCare` | `org.openphc.cce.episodeofcare` |

The Collector does not restrict resource types — any valid FHIR R4 resource is accepted.

## 11. Compliance Service Contract

The Compliance Service consumes CloudEvents JSON objects from `cce.events.inbound` with these guarantees from the Collector:

1. **`subject` is always present** — used for patient protocol instance lookup
2. **`type` follows `org.openphc.cce.<resource>`** — strictly validated (not normalized by the Collector; emitter adaptors must send the correct format)
3. **`data` contains a valid payload** — FHIR R4 resource (parseable via HAPI FHIR) when `datacontenttype` is `application/fhir+json`; valid JSON object when `datacontenttype` is `application/json`
4. **Field names use CloudEvents spec convention (lowercase)** — e.g., `specversion`, `datacontenttype`, `correlationid`
5. **Kafka key is `subject`** — per-patient ordering
6. **`correlationid` is always present** — for distributed tracing
7. **Each message maps to an `inbound_event` row** — authoritative source of truth

The Collector does NOT populate `protocolinstanceid`, `protocoldefinitionid`, or `actionid` — the Compliance Service resolves these independently.
