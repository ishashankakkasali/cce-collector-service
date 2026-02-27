# CCE Collector Service — AI Agent Instructions (Standalone Repo)

## Project Overview

This is the **Collector Service** — the event ingestion gateway of the Care Coordination Engine (CCE). It is built and deployed as a standalone Spring Boot application in its own repository. It receives clinical events from external EHR/RHIE systems (via openHIM mediators or direct integrations), validates them as CloudEvents v1.0 envelopes with FHIR R4 or JSON payloads, and publishes them synchronously to Kafka for downstream processing by the Compliance Service.

**Upstream design documents (maintained in the `cce-compliance-sub_system` repo):**
- `CCE Solution Design v0.3 Draft.pdf` — full architecture, event model, matching logic, APIs
- `CCE_Technology_Stack_Proposal.md` — tech choices, DB schema, infrastructure

---

## What This Service Does

The Collector Service is responsible for:
1. **Receiving clinical events** via REST API from external systems (openHIM RHIE mediators, EMR direct push, CHW apps)
2. **Validating CloudEvents envelope** — mandatory fields (`specversion`, `id`, `source`, `type`, `subject`), extensions, structure
3. **Validating event types** — strictly validating that `type` matches `org.openphc.cce.<resource>` pattern (normalization is the emitter adaptor's responsibility)
4. **Enriching defaults** — generating missing `correlationid` and `time` fields
5. **Validating payloads** — FHIR R4 structural validation (when `datacontenttype` = `application/fhir+json`) or JSON validity check (when `datacontenttype` = `application/json`)
6. **Deduplicating inbound events** — rejecting duplicate submissions using `(cloudevents_id, source)` compound key
7. **Publishing validated events** synchronously to Kafka topic `cce.events.inbound` with `subject` (patient UPID) as partition key
8. **Tracking rejected events** — recording rejection reason, failure stage, and error details on the `inbound_event` record for audit and retry
9. **Serving health/readiness endpoints** for orchestration platforms (Kubernetes, Docker)

**What this service does NOT do:**
- Event type normalization (emitter adaptors / openHIM mediators handle this)
- Protocol matching, step completion, or compliance tracking (Compliance Service)
- Time-based state transitions (Scheduler Service)
- OAuth token management, routing, or rate limiting (Gateway Service)
- Analytics or reporting (Analytics Service)
- Intelligence event delivery or action execution (Intelligence Subsystem — out of scope)

---

## Position in the CCE Platform

```
┌─────────────────────────────────────────────────────────────┐
│                    External Systems                          │
│  eBUZIMA EMR  │  SmartCare  │  CHW App  │  Lab Systems      │
└──────┬────────┴──────┬──────┴─────┬─────┴──────┬────────────┘
       │               │            │            │
       ▼               ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│               openHIM / RHIE Mediator Layer                  │
│    (routes, transforms, normalizes event types to            │
│     org.openphc.cce.<resource>, adds correlation IDs)        │
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

The Collector Service is the **single point of entry** for all clinical events into the CCE platform. No external system publishes directly to Kafka — all events flow through the Collector.

---

## Technology Stack

| Concern | Technology | Version |
|---------|------------|---------|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 3.4.x |
| Build tool | Maven | 3.9+ |
| Database | PostgreSQL | 16+ (latest stable) |
| Message broker | Apache Kafka | 3.7+ (KRaft mode) |
| FHIR library | HAPI FHIR | 7.4.0 |
| DB access | Spring Data JPA + Hibernate | (Spring Boot managed) |
| DB migration | Flyway | (Spring Boot managed) |
| Connection pool | HikariCP | (Spring Boot default) |
| Observability | Micrometer + OpenTelemetry | (Spring Boot managed) |
| Testing | JUnit 5, Testcontainers, MockMvc | |

### Key Maven Dependencies

```xml
<!-- HAPI FHIR (validation of inbound FHIR payloads) -->
<dependency>
    <groupId>ca.uhn.hapi.fhir</groupId>
    <artifactId>hapi-fhir-structures-r4</artifactId>
    <version>7.4.0</version>
</dependency>
<dependency>
    <groupId>ca.uhn.hapi.fhir</groupId>
    <artifactId>hapi-fhir-validation</artifactId>
    <version>7.4.0</version>
</dependency>
<dependency>
    <groupId>ca.uhn.hapi.fhir</groupId>
    <artifactId>hapi-fhir-validation-resources-r4</artifactId>
    <version>7.4.0</version>
</dependency>

<!-- Spring Boot starters -->
<!-- spring-boot-starter-web, spring-boot-starter-data-jpa, spring-kafka,
     spring-boot-starter-actuator,
     spring-boot-starter-validation -->
```

---

## Recommended Project Structure

```
cce-collector-service/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/org/openphc/cce/collector/
│   │   │   ├── CollectorServiceApplication.java
│   │   │   ├── config/
│   │   │   │   ├── KafkaProducerConfig.java
│   │   │   │   ├── FhirConfig.java              # FhirContext.forR4() singleton bean
│   │   │   │   ├── JpaConfig.java
│   │   │   │   ├── SecurityConfig.java           # Security configuration (auth delegated to Gateway)
│   │   │   │   └── WebConfig.java                # CORS, request logging
│   │   │   ├── domain/
│   │   │   │   ├── model/
│   │   │   │   │   ├── InboundEvent.java         # Single table: audit, dedup, rejection tracking, Kafka source
│   │   │   │   │   └── enums/
│   │   │   │   │       ├── InboundStatus.java     # RECEIVED, ACCEPTED, REJECTED, DUPLICATE
│   │   │   │   │       ├── RejectionReason.java   # INVALID_ENVELOPE, INVALID_EVENT_TYPE, INVALID_FHIR,
│   │   │   │   │       │                          # INVALID_JSON, UNSUPPORTED_CONTENT_TYPE, DUPLICATE,
│   │   │   │   │       │                          # MISSING_SUBJECT, PAYLOAD_TOO_LARGE,
│   │   │   │   │       │                          # DESERIALIZATION_ERROR, KAFKA_PUBLISH_FAILURE
│   │   │   │   │       └── FailureStage.java      # VALIDATION, PROCESSING, KAFKA_PUBLISH
│   │   │   │   └── repository/
│   │   │   │       └── InboundEventRepository.java
│   │   │   ├── service/
│   │   │   │   ├── EventIngestionService.java    # Main orchestrator: validate → persist → publish
│   │   │   │   ├── CloudEventValidator.java      # CloudEvents v1.0 envelope validation
│   │   │   │   ├── EventTypeValidator.java        # Strict org.openphc.cce.<resource> pattern validation
│   │   │   │   ├── EventDefaultsEnricher.java     # Generate correlationid, fill time if absent
│   │   │   │   ├── FhirPayloadValidator.java     # FHIR R4 structural validation via HAPI
│   │   │   │   ├── DeduplicationService.java      # (cloudevents_id, source) dedup via PostgreSQL with lookback window
│   │   │   │   └── RejectionService.java          # Record rejection details on inbound_event
│   │   │   ├── kafka/
│   │   │   │   └── InboundEventProducer.java      # Publishes to cce.events.inbound (synchronous)
│   │   │   ├── api/
│   │   │   │   ├── controller/
│   │   │   │   │   ├── EventIngestionController.java  # POST /v1/events — main entry point
│   │   │   │   │   └── RejectedEventController.java   # GET /v1/events/rejected — view/retry rejected events
│   │   │   │   ├── dto/
│   │   │   │   │   ├── ApiResponse.java               # { "data": ... } envelope
│   │   │   │   │   ├── ApiError.java                  # { "error": { "code", "message" } }
│   │   │   │   │   ├── EventIngestionRequest.java     # CloudEvents envelope (inbound DTO)
│   │   │   │   │   ├── EventIngestionResponse.java    # Accepted/rejected receipt
│   │   │   │   │   └── RejectedEventDto.java
│   │   │   │   └── exception/
│   │   │   │       └── GlobalExceptionHandler.java
│   │   │   └── fhir/
│   │   │       ├── FhirResourceParser.java        # HAPI FHIR parse + type detection
│   │   │       └── FhirResourceValidator.java     # Profile validation (optional, extensible)
│   │   └── resources/
│   │       ├── application.yml
│   │       ├── application-local.yml
│   │       ├── application-staging.yml
│   │       ├── application-production.yml
│   │       └── db/migration/
│   │           └── V1__create_inbound_event.sql
│   └── test/
│       └── java/org/openphc/cce/collector/
│           ├── service/
│           ├── api/
│           ├── kafka/
│           ├── fhir/
│           └── integration/
├── Dockerfile
├── docker-compose.yml                               # local dev: PostgreSQL, Kafka
├── .github/
│   └── copilot-instructions-collector-service.md    # this file
└── README.md
```

---

## Database Schema

The Collector Service uses a **single table** design. All state is tracked on `inbound_event`. Use Flyway for schema migrations.

### `inbound_event` — Request Audit Log & Rejection Tracking

Every HTTP request is persisted **as-is** before processing. Used for audit trail, primary deduplication, rejection tracking, and as the source of data for Kafka messages. Rejected events are recorded directly on this table — there is no separate dead letter table.

```sql
CREATE TABLE inbound_event (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloudevents_id    VARCHAR NOT NULL,
    source            VARCHAR NOT NULL,
    type              VARCHAR NOT NULL,               -- event type as received (must already be org.openphc.cce.<resource>)
    spec_version      VARCHAR NOT NULL DEFAULT '1.0',
    subject           VARCHAR,                        -- patient UPID (e.g., '260225-0002-5501')
    event_time        TIMESTAMPTZ,                    -- CloudEvents time attribute
    data_content_type VARCHAR DEFAULT 'application/fhir+json',
    facility_id       VARCHAR,                        -- healthcare facility FOSA ID
    correlation_id    VARCHAR,                        -- distributed tracing ID (may be null if not provided by source)
    source_event_id   VARCHAR,                        -- source system's internal event ID
    raw_payload       JSONB NOT NULL,                 -- full original request body as received (immutable)
    status            VARCHAR NOT NULL DEFAULT 'RECEIVED',  -- see InboundStatus enum
    rejection_reason  VARCHAR,                        -- reason code if status = 'REJECTED' (see RejectionReason enum)
    failure_stage     VARCHAR,                        -- pipeline stage where failure occurred (see FailureStage enum)
    error_details     TEXT,                           -- stack trace or validation error messages
    resolved          BOOLEAN NOT NULL DEFAULT false,  -- whether a rejected event has been resolved/acknowledged
    resolved_at       TIMESTAMPTZ,                    -- resolution timestamp
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),  -- server-side receipt timestamp (UTC)
    CONSTRAINT uq_inbound_event_id_source UNIQUE (cloudevents_id, source)
);

CREATE INDEX idx_inbound_event_subject     ON inbound_event (subject);
CREATE INDEX idx_inbound_event_source      ON inbound_event (source);
CREATE INDEX idx_inbound_event_status      ON inbound_event (status);
CREATE INDEX idx_inbound_event_received    ON inbound_event (received_at);
CREATE INDEX idx_inbound_event_rejection   ON inbound_event (rejection_reason);
CREATE INDEX idx_inbound_event_unresolved  ON inbound_event (status, resolved)
    WHERE status = 'REJECTED' AND resolved = false;
```

### Key Design Decisions

- **Single table** — no separate `event_log` or `dead_letter_event` tables. The `inbound_event` table serves all roles: audit trail, dedup source, rejection tracking, and data source for Kafka messages.
- **No outbox pattern** — Kafka publish is synchronous. On failure, the service returns HTTP 500 and the source system retries.
- **Rejection tracking columns** (`rejection_reason`, `failure_stage`, `error_details`, `resolved`, `resolved_at`) are directly on `inbound_event`, eliminating the need for a dead letter table.

### Schema Conventions

- All PKs are `UUID DEFAULT gen_random_uuid()`
- All timestamps are `TIMESTAMPTZ` (always stored/queried in UTC)
- Status/state columns use application-level enum validation
- JSONB columns use GIN indexes with `jsonb_path_ops` when queried
- Internal DB columns use `snake_case`
- CloudEvents extension attributes use `lowercase` (no separators) per CloudEvents spec
- Use Flyway versioned migrations (`V1__`, etc.) — never modify existing migrations

---

## REST API Endpoints

The Collector Service exposes its ingestion API to external systems. In production, the CCE Gateway terminates TLS, validates tokens, and routes requests to the Collector.

### Event Ingestion (Primary)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/events` | Ingest a single clinical event (CloudEvents envelope) |

### Rejected Event Management

| Method | Path | Scope | Description |
|--------|------|-------|-------------|
| GET | `/v1/events/rejected` | `collector:admin` | List rejected events (filter by reason, source, resolved status) |
| GET | `/v1/events/rejected/{id}` | `collector:admin` | Get a specific rejected event |
| POST | `/v1/events/rejected/{id}/retry` | `collector:admin` | Re-process a rejected event through the validation pipeline |

### Response Envelope

```json
// Success — single event accepted
{
  "data": {
    "eventId": "evt-2026-03-15-001",
    "status": "accepted",
    "correlationId": "corr-abc-123-def-456",
    "timestamp": "2026-03-15T10:30:01Z"
  }
}

// Error
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Missing required CloudEvents field: 'type'",
    "details": { "field": "type" }
  }
}
```

---

## CloudEvents v1.0 Envelope Specification

### Inbound Event Structure

All inbound events MUST conform to CloudEvents v1.0 with CCE extensions. Field names use **lowercase** per the CloudEvents spec — this is preserved end-to-end through to the Kafka message (no field name translation is performed).

```json
{
  "specversion": "1.0",
  "id": "evt-a1b2c3d4-1111-4000-8000-000000000001",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.encounter",
  "subject": "260115-0001-7823",
  "time": "2026-01-15T08:30:00Z",
  "datacontenttype": "application/fhir+json",

  "facilityid": "0001",
  "correlationid": "corr-95fe0bdb-6462-5f36-ba91-c18caca81cd2",
  "sourceeventid": "enc-visit-20260129-001",

  "data": {
    "resourceType": "Encounter",
    "id": "9a8e5398-aaaa-4111-84a0-9e1e6e0a0001",
    "status": "finished",
    "class": { ... },
    "type": [ ... ],
    "subject": { "reference": "Patient/260115-0001-7823" }
  }
}
```

### Field Validation Rules

| Field | Required | Validation | Notes |
|-------|----------|------------|-------|
| `specversion` | **Yes** | Must be `"1.0"` | CloudEvents spec version |
| `id` | **Yes** | Non-empty string, max 256 chars | Globally unique event identifier |
| `source` | **Yes** | Non-empty string (URI or short identifier) | Event source identifier (e.g., `"rhie-mediator"`) |
| `type` | **Yes** | Must match `org.openphc.cce.<resource>` pattern | Strictly validated — rejected if non-conforming |
| `subject` | **Yes** (CCE requirement) | Non-empty string (patient UPID) | Kafka partition key; absent `subject` → reject |
| `time` | Recommended | ISO 8601 datetime | If absent, Collector fills with `received_at` |
| `datacontenttype` | Recommended | `application/fhir+json` or `application/json` | If absent, defaults to `application/fhir+json`; other values → reject |
| `facilityid` | Recommended | Non-empty string (FOSA ID) | Healthcare facility identifier |
| `correlationid` | Recommended | UUID string | If absent, Collector generates `corr-<uuid>` |
| `sourceeventid` | Optional | String | Source system's internal event ID |
| `data` | **Yes** (CCE requirement) | Must be a valid JSON object | FHIR validation applied when `datacontenttype` = `application/fhir+json`; JSON validity check when `application/json` |

### Event Type Validation (NOT Normalization)

The Collector **does not normalize** event types — normalization is the responsibility of the emitter adaptor (openHIM mediator layer). The Collector **strictly validates** that the `type` field matches the required pattern and rejects non-conforming events.

**Required format:** `org.openphc.cce.<resource>` (where `<resource>` is a lowercase FHIR R4 resource type)

| Inbound `type` Value | Valid? | Action |
|----------------------|--------|--------|
| `org.openphc.cce.encounter` | Yes | Accepted — passes through unchanged |
| `org.openphc.cce.observation` | Yes | Accepted — passes through unchanged |
| `cce.encounter.created` | **No** | Rejected — `INVALID_EVENT_TYPE`, 400 Bad Request |
| `cce.observation.updated` | **No** | Rejected — `INVALID_EVENT_TYPE`, 400 Bad Request |
| `custom.event.type` | **No** | Rejected — `INVALID_EVENT_TYPE`, 400 Bad Request |

> **Emitter adaptors** (openHIM mediators) are responsible for mapping source-system event types to the `org.openphc.cce.<resource>` format before submitting to the Collector.

### CCE Extension Attributes

Per the CloudEvents specification, extension attribute names are `lowercase` with no separators:

| Extension | Key | Purpose |
|-----------|-----|---------|
| Facility ID | `facilityid` | Healthcare facility where the event occurred |
| Correlation ID | `correlationid` | Distributed tracing ID across the RHIE/CCE ecosystem |
| Source Event ID | `sourceeventid` | Original event ID from the source system |
| Protocol Instance ID | `protocolinstanceid` | Pre-populated if source knows the target protocol instance (usually null) |
| Protocol Definition ID | `protocoldefinitionid` | Pre-populated if source knows the target protocol (usually null) |
| Action ID | `actionid` | Pre-populated if source knows the target action/step (usually null) |

---

## Content Type Handling

The Collector supports two content types for the `data` payload, branching validation accordingly:

| `datacontenttype` | Validation | On Failure |
|--------------------|-----------|------------|
| `application/fhir+json` (or absent) | Full FHIR R4 structural validation via HAPI | 422 `INVALID_FHIR` |
| `application/json` | JSON validity check (must be non-empty valid JSON object) | 422 `INVALID_JSON` |
| Any other value | Rejected immediately | 400 `UNSUPPORTED_CONTENT_TYPE` |

---

## Core Processing Algorithm

### Event Ingestion Flow (EventIngestionService)

```
1. Receive HTTP POST → parse request body
2. CloudEvents Envelope Validation
   a. Required fields: specversion, id, source, type, subject, data
   b. specversion must be "1.0"
   c. subject must be non-empty (patient UPID required by CCE)
   d. If validation fails → 400 response, persist inbound_event with status=REJECTED
3. Deduplication Check
   a. Query: check if (cloudevents_id, source) exists within lookback window
   b. If duplicate → return 200 OK with status: "duplicate"
   c. If not found → proceed (DB unique constraint is the authoritative check)
4. Persist to inbound_event table (status = 'RECEIVED', raw_payload = original request body)
   — This is the first write: raw audit record before any validation
5. Event Type Validation
   a. Strictly validate type matches org.openphc.cce.<resource> pattern
   b. If invalid → update inbound_event status=REJECTED, reason=INVALID_EVENT_TYPE, return 400
6. Default Enrichment
   a. Generate correlationid if absent (new UUID with "corr-" prefix)
   b. Fill time with server received_at if absent
7. Payload Validation (branched by datacontenttype)
   a. application/fhir+json (or absent):
      - Parse data via HAPI FHIR: fhirContext.newJsonParser().parseResource(json)
      - Validate resourceType is present and parseable
      - If invalid → update inbound_event status=REJECTED, reason=INVALID_FHIR, return 422
   b. application/json:
      - Validate data is a non-empty valid JSON object
      - If invalid → update inbound_event status=REJECTED, reason=INVALID_JSON, return 422
   c. Other:
      - Reject → update inbound_event status=REJECTED, reason=UNSUPPORTED_CONTENT_TYPE, return 400
8. Update inbound_event status = 'ACCEPTED'
9. Synchronous Kafka Publish
   a. Key = subject (patient UPID) — guarantees per-patient message ordering
   b. Value = CloudEvents JSON with lowercase field names (built from inbound_event fields)
   c. On success → return HTTP 202 Accepted with ingestion receipt
   d. On failure → update inbound_event status=REJECTED, reason=KAFKA_PUBLISH_FAILURE,
      return HTTP 500 Internal Server Error
      (source system is expected to retry the request)
```

> **No outbox pattern.** Kafka publish is synchronous and inline with request processing. There is no background retry or scheduled publisher. If Kafka is unavailable, the service returns 500 and the source system retries.

### Kafka Publish Contract

The Collector publishes a **CloudEvents JSON object** to `cce.events.inbound`. Field names use **lowercase** per the CloudEvents spec — no field name translation is performed. The Kafka message is built from `inbound_event` fields.

```json
{
  "id": "evt-a1b2c3d4-1111-4000-8000-000000000001",
  "source": "rhie-mediator",
  "type": "org.openphc.cce.encounter",
  "specversion": "1.0",
  "subject": "260115-0001-7823",
  "time": "2026-01-15T08:30:00Z",
  "datacontenttype": "application/fhir+json",
  "correlationid": "corr-95fe0bdb-6462-5f36-ba91-c18caca81cd2",
  "sourceeventid": "enc-visit-20260129-001",
  "protocolinstanceid": null,
  "protocoldefinitionid": null,
  "actionid": null,
  "facilityid": "0001",
  "data": { ... }
}
```

> **Note:** `null` fields are omitted from the JSON output (`@JsonInclude(NON_NULL)`).

### Transaction Boundaries

- Use `@Transactional` on service methods that write to the database
- **Single table, synchronous publish:**
  1. **Transaction 1:** Insert `inbound_event` (status = `RECEIVED`) — immediate audit trail
  2. **Transaction 2:** Validate, update `inbound_event.status = 'ACCEPTED'`
  3. **After commit:** Publish to Kafka synchronously via `KafkaTemplate.send()`
  4. **On Kafka success:** Return HTTP 202 Accepted
  5. **On Kafka failure:** Update `inbound_event.status = 'REJECTED'`, `rejection_reason = 'KAFKA_PUBLISH_FAILURE'` — return HTTP 500
- Keep transactions short — avoid holding DB locks during Kafka operations

---

## Kafka Integration

### Topics This Service Produces

| Topic | Key | Purpose |
|-------|-----|---------|
| `cce.events.inbound` | `subject` (patient UPID) | Validated clinical events for Compliance Service |
| `cce.deadletter` | `correlationid` | Optional: rejected event summaries for external monitoring |

> **Note:** The primary rejection record is stored in `inbound_event` (with `status = REJECTED`, `rejection_reason`, `failure_stage`, and `error_details`). Publishing to `cce.deadletter` is optional and intended for external monitoring systems that consume Kafka rather than query the database.

### This Service Does NOT Consume Any Kafka Topics

The Collector is a **producer-only** Kafka participant. It receives events via HTTP, not Kafka.

### Kafka Producer Configuration

```yaml
spring.kafka:
  bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
  producer:
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    acks: all
    retries: 3
    properties:
      enable.idempotence: true
      max.in.flight.requests.per.connection: 5
      linger.ms: 5
      batch.size: 16384
```

| Setting | Value | Rationale |
|---------|-------|-----------|
| `acks` | `all` | Wait for all in-sync replicas to acknowledge |
| `retries` | `3` | Retry on transient failures (producer-level) |
| `enable.idempotence` | `true` | Prevent duplicate messages on producer retry |
| `linger.ms` | `5` | Small batching window for throughput |
| `batch.size` | `16384` | 16KB batch size (default) |

### Kafka Producer Implementation

```java
@Service
@RequiredArgsConstructor
public class InboundEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${cce.kafka.topics.inbound}")
    private String inboundTopic;

    public void publish(CloudEventMessage event) {
        String key = event.getSubject();  // Partition by patient UPID
        try {
            kafkaTemplate.send(inboundTopic, key, event).get();  // Synchronous — blocks until ack
            log.info("Published event {} to {}", event.getId(), inboundTopic);
        } catch (Exception ex) {
            log.error("Failed to publish event {} to Kafka", event.getId(), ex);
            throw new KafkaPublishException(event, ex);
        }
    }
}
```

### Topic Configuration

The `cce.events.inbound` topic:
- **12 partitions** — supports up to 12 parallel Compliance Service consumers
- **Replication factor: 3** (production) / 1 (local dev)
- **Retention: 7 days** — events are replayable for a week
- **Partition key:** `subject` (patient UPID) — guarantees per-patient ordering
- **Cleanup policy:** `delete`

---

## FHIR Handling

### FhirContext Configuration

```java
@Configuration
public class FhirConfig {
    @Bean
    public FhirContext fhirContext() {
        return FhirContext.forR4();  // Thread-safe singleton — expensive to create
    }
}
```

- **NEVER** hand-parse FHIR JSON — always use `fhirContext.newJsonParser().parseResource()`
- `FhirContext` is expensive to create — instantiate once as a Spring Bean
- Use `IParser.setPrettyPrint(false)` for Kafka messages (minimize payload size)

### FHIR Validation Strategy

The Collector performs **structural validation**, not clinical validation. FHIR validation is only applied when `datacontenttype` = `application/fhir+json`:

| Check | Level | Action on Failure |
|-------|-------|-------------------|
| `data` parses as valid JSON | Required | Reject with `INVALID_FHIR` |
| `data.resourceType` is present and non-empty | Required | Reject with `INVALID_FHIR` |
| HAPI FHIR can parse into an `IBaseResource` | Required | Reject with `INVALID_FHIR` |
| `data.resourceType` is a known FHIR R4 type | Warning | Accept but log warning |
| `data.subject.reference` contains patient ID matching `subject` | Warning | Accept but log warning (may be implicit) |
| FHIR profile conformance | Optional (future) | Accept but include validation warnings in response |

### Supported FHIR Resource Types

Events from existing CCE sources contain these resource types:

| Resource Type | Event Type | Example |
|---------------|------------|---------|
| `Encounter` | `org.openphc.cce.encounter` | Visit registration, consultation |
| `Observation` | `org.openphc.cce.observation` | Vital signs, lab results |
| `Condition` | `org.openphc.cce.condition` | Diagnoses |
| `MedicationRequest` | `org.openphc.cce.medicationrequest` | Prescriptions |
| `MedicationDispense` | `org.openphc.cce.medicationdispense` | Pharmacy dispensing |
| `ServiceRequest` | `org.openphc.cce.servicerequest` | Lab/imaging orders |
| `Procedure` | `org.openphc.cce.procedure` | Clinical procedures |
| `EpisodeOfCare` | `org.openphc.cce.episodeofcare` | ANC enrollment, program enrollment |
| `DiagnosticReport` | `org.openphc.cce.diagnosticreport` | Lab and imaging reports |
| `Immunization` | `org.openphc.cce.immunization` | Vaccinations |
| `AllergyIntolerance` | `org.openphc.cce.allergyintolerance` | Allergy records |
| `CarePlan` | `org.openphc.cce.careplan` | Treatment plans |
| `Patient` | `org.openphc.cce.patient` | Patient demographics |

The Collector does not restrict resource types — any valid FHIR R4 resource is accepted.

---

## Deduplication Strategy

Deduplication uses a PostgreSQL unique constraint as the authoritative layer, with a configurable lookback query for performance.

### PostgreSQL Lookback Query

On event arrival, the service queries PostgreSQL to check if a record with the same `(cloudevents_id, source)` exists within the configured lookback window (default: 30 days):

```java
public boolean isDuplicate(String source, String cloudeventsId) {
    OffsetDateTime since = OffsetDateTime.now().minusDays(lookbackDays);
    return inboundEventRepository
        .existsByCloudeventsIdAndSourceAndReceivedAtAfter(cloudeventsId, source, since);
}
```

The lookback window is configurable via `cce.collector.dedup.lookback-days` (default: 30 days). This limits the dedup query scope instead of scanning the entire database.

### PostgreSQL Unique Constraint (Authoritative)

```sql
CONSTRAINT uq_inbound_event_id_source UNIQUE (cloudevents_id, source)
```

On duplicate insert, PostgreSQL raises a constraint violation → the service returns 200 with `status: "duplicate"` (idempotent response).

### Idempotency Contract

- Submitting the same event twice (same `id` + `source`) returns **200 OK** (not 409 Conflict)
- The response includes `status: "duplicate"` so callers know it was already processed
- The event is **not** re-published to Kafka on duplicate submission
- This follows the standard idempotent POST pattern used by openHIM mediators

---

## Default Enrichment

The Collector enriches events with server-generated defaults when optional fields are missing. This is **not normalization** — the event type and other source-provided fields are never modified.

### Correlation ID Generation

If the inbound event is missing `correlationid`, the Collector generates one:

```java
public String ensureCorrelationId(String existing) {
    return (existing != null && !existing.isBlank())
        ? existing
        : "corr-" + UUID.randomUUID();
}
```

### Time Fill

If the inbound event is missing `time`, fill with the server's received timestamp:

```java
public OffsetDateTime ensureEventTime(String rawTime) {
    return (rawTime != null && !rawTime.isBlank())
        ? OffsetDateTime.parse(rawTime)
        : OffsetDateTime.now(ZoneOffset.UTC);
}
```

---

## Authentication & Security

### Authentication

Authentication is **not handled directly** by the Collector Service — the CCE Gateway Service validates tokens and authorizes operations before forwarding requests to the Collector (per CCE Solution Design v0.3 Section 7.2.1).

### mTLS (Production)

In production deployments behind an openHIM mediator, the Collector validates the client certificate presented by the mediator. TLS termination happens at the infrastructure layer (load balancer / service mesh).

### Rate Limiting

Per-source rate limiting to prevent a single source from overwhelming the system:

| Source Type | Default Limit | Window |
|-------------|---------------|--------|
| openHIM mediator | 1000 events/min | 1 minute sliding |
| Direct EMR integration | 200 events/min | 1 minute sliding |

Rate limits are configurable per source via application configuration.

---

## Observability

### Metrics to Instrument

```java
// Events received by source and outcome
Counter.builder("cce.collector.events.received")
    .tag("source", source)
    .tag("status", "accepted")  // or rejected, duplicate
    .register(meterRegistry);

// FHIR validation outcomes
Counter.builder("cce.collector.fhir.validation")
    .tag("result", "valid")  // or invalid
    .tag("resourceType", "Encounter")
    .register(meterRegistry);

// Kafka publish latency
Timer.builder("cce.collector.kafka.publish.duration")
    .register(meterRegistry);

// Kafka publish failures
Counter.builder("cce.collector.kafka.publish.failures")
    .register(meterRegistry);

// Unresolved rejected events
Gauge.builder("cce.collector.rejected.unresolved", inboundEventRepository,
    r -> r.countByStatusAndResolved("REJECTED", false))
    .register(meterRegistry);

// Event ingestion latency (end-to-end: receive → publish)
Timer.builder("cce.collector.ingestion.duration")
    .register(meterRegistry);
```

### Health Endpoints

- `/actuator/health` — includes Kafka producer, PostgreSQL connectivity
- `/actuator/health/liveness` — basic liveness (JVM is running)
- `/actuator/health/readiness` — readiness (Kafka producer connected, DB accessible)
- `/actuator/prometheus` — Micrometer metrics for Prometheus scraping

---

## Code Generation Guidelines

### General Patterns

- Use **constructor injection** exclusively — no `@Autowired` on fields
- Use `@RequiredArgsConstructor` (Lombok) or explicit constructors
- Entity classes: JPA `@Entity` with Hibernate
- Use `@Column(columnDefinition = "jsonb")` for JSONB fields
- Use `@Enumerated(EnumType.STRING)` for enum columns
- Use `Optional<T>` for nullable return values from repositories
- Use Java records for DTOs and value objects where appropriate
- Validate inputs with Bean Validation (`@Valid`, `@NotNull`, `@NotBlank`)

### REST Controller Pattern

```java
@RestController
@RequestMapping("/v1/events")
@RequiredArgsConstructor
public class EventIngestionController {

    private final EventIngestionService ingestionService;

    @PostMapping
    public ResponseEntity<ApiResponse<EventIngestionResponse>> ingestEvent(
            @Valid @RequestBody EventIngestionRequest request,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {

        MDC.put("correlationId", correlationId);
        try {
            EventIngestionResponse response = ingestionService.ingest(request);
            HttpStatus status = response.isDuplicate()
                ? HttpStatus.OK
                : HttpStatus.ACCEPTED;  // 202 on success
            return ResponseEntity.status(status)
                .body(ApiResponse.success(response));
        } finally {
            MDC.clear();
        }
    }
}
```

### Error Handling

- `400 VALIDATION_ERROR` — invalid CloudEvents envelope, missing required fields, invalid event type
- `401 UNAUTHORIZED` — missing or invalid API key
- `403 FORBIDDEN` — source not registered or inactive
- `413 PAYLOAD_TOO_LARGE` — request body exceeds max size (1 MB)
- `422 UNPROCESSABLE_ENTITY` — FHIR payload or JSON validation failure
- `429 TOO_MANY_REQUESTS` — rate limit exceeded
- `500 INTERNAL_SERVER_ERROR` — unexpected failures or Kafka publish failure
- `503 SERVICE_UNAVAILABLE` — DB not reachable
- Use `@ControllerAdvice` with `@ExceptionHandler` for centralized error handling
- Always return the `{ "error": { "code", "message" } }` envelope

---

## Testing Strategy

### Unit Tests
- `CloudEventValidator` — envelope validation with valid/invalid permutations
- `EventTypeValidator` — strict `org.openphc.cce.<resource>` pattern matching
- `EventDefaultsEnricher` — correlation ID generation, time fill
- `FhirPayloadValidator` — FHIR parsing with valid/invalid/edge-case payloads
- `DeduplicationService` — duplicate detection logic

### Integration Tests
- Use **Testcontainers** for PostgreSQL and Kafka
- Test full ingestion flow: HTTP POST → validation → DB persist → Kafka publish → verify message on topic
- Test deduplication: submit same event twice, verify idempotent response
- Test rejection tracking: invalid events recorded with correct rejection reasons on `inbound_event`
- Test Kafka failure: verify 500 response and `KAFKA_PUBLISH_FAILURE` rejection when Kafka is down

### API Tests
- Use `@WebMvcTest` + `MockMvc` for controller tests
- Verify response envelopes, HTTP status codes, error formats
- Test API key authentication (valid, invalid, missing)
- Test rate limiting (if implemented)

### Test Fixtures
- Maintain sample CloudEvents JSON files in `src/test/resources/fixtures/`
- Include: valid encounter event, valid observation event, invalid envelope (missing type), invalid FHIR payload, invalid event type, non-FHIR JSON event, duplicate event
- Reference sample events from `artifacts/sample-kafka-events-ebuzima-visit.json` and `artifacts/sample-kafka-events-rhie.json` for realistic test data

---

## Configuration (application.yml)

```yaml
spring:
  application:
    name: cce-collector-service
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:cce_collector}
    username: ${DB_USER:cce_collector}
    password: ${DB_PASSWORD:cce_collector}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
  jpa:
    hibernate:
      ddl-auto: validate  # Flyway manages schema
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
  flyway:
    enabled: true
    locations: classpath:db/migration
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5

cce:
  kafka:
    topics:
      inbound: cce.events.inbound
      dead-letter: cce.deadletter             # optional monitoring topic
  collector:
    max-payload-size: 1048576                  # 1 MB for single event
    fhir-validation:
      enabled: true
      strict-mode: false                       # true = reject on any warning; false = warnings logged only
    dedup:
      lookback-days: 30                        # Number of days to check for duplicates

server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,prometheus,info
  endpoint:
    health:
      probes:
        enabled: true
      show-details: always
  metrics:
    tags:
      application: cce-collector-service
```

---

## Docker & Local Development

### docker-compose.yml (local dev)

Include PostgreSQL 16 and Kafka (KRaft, single broker) for local development. Flyway runs automatically on application startup.

```yaml
version: '3.8'

services:
  cce-collector-postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: cce_collector
      POSTGRES_USER: cce_collector
      POSTGRES_PASSWORD: cce_collector
    ports:
      - "5433:5432"
    volumes:
      - collector-pgdata:/var/lib/postgresql/data

  cce-kafka:
    image: apache/kafka:3.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
    ports:
      - "9092:9092"

volumes:
  collector-pgdata:
```

Note: If running alongside the Compliance Service locally, share the same Kafka container. The Collector uses a **separate PostgreSQL database** (`cce_collector` on port 5433) from the Compliance Service (`cce` on port 5432).

### Dockerfile

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/cce-collector-service-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Use multi-stage build for production: builder stage with Maven + JDK, runtime stage with JRE-alpine only.

---

## Deployment Notes

- **Stateless** — no in-memory state between requests. All state is in PostgreSQL.
- **Horizontally scalable** — add instances behind a load balancer
- **Initial deployment:** 2 replicas behind an L4/L7 load balancer
- Resource allocation: 0.5 CPU request, 512 MB memory request per instance
- Collector runs on port **8080** (same port as other CCE services; Gateway routes by hostname)
- PgBouncer in transaction-mode pooling recommended for connection management
- In production, the CCE Gateway routes `/v1/events*` to the Collector Service
- Authentication is handled by the CCE Gateway Service (not the Collector directly)
- **No partition management required** — `inbound_event` is a standard (non-partitioned) table

---

## Interaction Contract with Compliance Service

The Collector and Compliance Service have a **loose coupling** via Kafka. The Collector's output is the Compliance Service's input.

### What the Compliance Service Expects

The Compliance Service consumes CloudEvents JSON objects from `cce.events.inbound` with these expectations:

1. **`subject` is always present** — used for patient protocol instance lookup
2. **`type` follows the `org.openphc.cce.<resource>` pattern** — used for Tier 1 structural matching in `trigger_index`
3. **`data` contains a valid payload** — FHIR R4 resource or valid JSON object depending on `datacontenttype`
4. **Field names use CloudEvents lowercase convention** — e.g., `specversion`, `datacontenttype`, `correlationid`
5. **Kafka key = `subject`** — ensures per-patient ordering across partitions
6. **`correlationid` is always present** — used for distributed tracing (MDC) in the Compliance Service
7. **Each Kafka message maps to an `inbound_event` row** — the Collector's `inbound_event` table is the authoritative source of truth

### What the Compliance Service Does NOT Expect

- The Collector does NOT need to populate `protocolinstanceid`, `protocoldefinitionid`, or `actionid` — the Compliance Service resolves these from the event data
- The Collector does NOT need to know about PlanDefinitions or protocol steps — it is purely an ingestion gateway
- The Collector does NOT need to verify that the patient exists or is enrolled — the Compliance Service handles zero-match cases gracefully

---

## Out of Scope (Explicit Exclusions)

- Event type normalization (emitter adaptor / openHIM mediator responsibility)
- Protocol matching, step completion, or compliance tracking (Compliance Service)
- Time-based transitions (Scheduler Service)
- OAuth/JWT token validation (Gateway Service handles this upstream)
- FHIR profile conformance validation (structural parse only — clinical validation is out of scope)
- Event transformation or enrichment beyond default fill (no external lookups, no patient registry calls)
- Event routing to multiple topics (all events go to `cce.events.inbound`)
- Field name translation (lowercase CloudEvents field names preserved end-to-end)
- Background retry or outbox pattern (synchronous publish only — source system retries on 500)
- WebSocket or streaming event ingestion (REST-only for now)
- Brownfield event backfill / historical data migration
