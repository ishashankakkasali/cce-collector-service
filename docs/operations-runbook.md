# CCE Collector Service — Operations Runbook

## 1. Health Checks

### Application Health

```bash
curl http://localhost:8080/actuator/health
```

Expected response:
```json
{
  "status": "UP",
  "components": {
    "db": { "status": "UP" },
    "kafka": { "status": "UP" },
    "diskSpace": { "status": "UP" }
  }
}
```

### Kubernetes Probes

| Probe | Endpoint | Purpose |
|-------|----------|---------|
| Liveness | `/actuator/health/liveness` | Is the process alive? |
| Readiness | `/actuator/health/readiness` | Can the service accept traffic? |

### Quick Diagnostics Script

```bash
# Check all infrastructure dependencies
echo "=== Application ==="
curl -s http://localhost:8080/actuator/health | jq .status

echo "=== PostgreSQL ==="
psql -h localhost -p 5433 -U cce_user -d cce_collector -c "SELECT 1;"

echo "=== Kafka ==="
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## 2. Monitoring & Metrics

### Prometheus Endpoint

```
GET /actuator/prometheus
```

### Key Metrics to Monitor

#### Ingestion Throughput

| Metric | Description |
|--------|-------------|
| `cce_events_received_total` | Total events received (counter) |
| `cce_events_accepted_total` | Events accepted (counter) |
| `cce_events_rejected_total` | Events rejected (counter) |
| `cce_events_duplicated_total` | Duplicate events detected (counter) |
| `cce_events_published_total` | Events published to Kafka (counter) |

#### Latency

| Metric | Description |
|--------|-------------|
| `http_server_requests_seconds` | HTTP request latency (histogram) |
| `cce_kafka_publish_seconds` | Kafka publish latency (histogram) |

#### Infrastructure

| Metric | Description |
|--------|-------------|
| `hikaricp_connections_active` | Active DB connections |
| `hikaricp_connections_idle` | Idle DB connections |
| `hikaricp_connections_pending` | Pending DB connection requests |
| `spring_kafka_producer_record_send_total` | Kafka records sent |

### Alerting Rules (Prometheus)

```yaml
groups:
  - name: cce-collector-alerts
    rules:
      - alert: HighRejectionRate
        expr: rate(cce_events_rejected_total[5m]) / rate(cce_events_received_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Event rejection rate > 10%"
          description: "More than 10% of events are being rejected. Check source systems."

      - alert: KafkaPublishFailures
        expr: rate(cce_events_published_total{status="failed"}[5m]) > 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Kafka publish failures detected"
          description: "Events failing to publish to Kafka. Check broker connectivity."

      - alert: RejectedEventBacklog
        expr: cce_rejected_events_unresolved_total > 100
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Unresolved rejected event backlog growing"

      - alert: DBConnectionPoolExhausted
        expr: hikaricp_connections_pending > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Database connection pool exhausted"
```

### Grafana Dashboard Panels

Recommended panels for a Collector Service dashboard:

1. **Events per second** — `rate(cce_events_received_total[1m])`
2. **Acceptance rate** — `rate(cce_events_accepted_total[1m]) / rate(cce_events_received_total[1m])`
3. **Duplicate rate** — `rate(cce_events_duplicated_total[1m]) / rate(cce_events_received_total[1m])`
4. **P95 ingestion latency** — `histogram_quantile(0.95, rate(http_server_requests_seconds_bucket{uri="/v1/events"}[5m]))`
5. **Kafka publish latency** — `histogram_quantile(0.95, rate(cce_kafka_publish_seconds_bucket[5m]))`
6. **Rejected event backlog** — `cce_rejected_events_unresolved_total`
7. **DB connection pool** — `hikaricp_connections_active` vs `hikaricp_connections_idle`

---

## 3. Rejected Event Management

### Overview

Rejected events are events that failed validation or processing. They are tracked in the `inbound_event` table with `status = 'REJECTED'`, along with `rejection_reason`, `failure_stage`, `error_details`, and `resolved` columns. Optionally, a summary is also published to the `cce.deadletter` Kafka topic for external monitoring.

### Querying Rejected Events

```bash
# List recent rejected events (paginated)
curl "http://localhost:8080/v1/events/rejected?page=0&size=20" | jq .

# Get a specific rejected event
curl "http://localhost:8080/v1/events/rejected/{id}" | jq .
```

### Direct Database Query

```sql
-- Count by rejection reason
SELECT rejection_reason, COUNT(*) 
FROM inbound_event 
WHERE status = 'REJECTED' AND resolved = false 
GROUP BY rejection_reason 
ORDER BY count DESC;

-- Recent failures
SELECT id, cloudevents_id, source, rejection_reason, failure_stage, error_details, received_at
FROM inbound_event 
WHERE status = 'REJECTED' AND resolved = false 
ORDER BY received_at DESC 
LIMIT 20;

-- Failures by source
SELECT source, COUNT(*) 
FROM inbound_event 
WHERE status = 'REJECTED' AND resolved = false
  AND received_at > NOW() - INTERVAL '24 hours'
GROUP BY source 
ORDER BY count DESC;
```

### Retry Rejected Events

```bash
# Retry a specific rejected event
curl -X POST "http://localhost:8080/v1/events/rejected/{id}/retry"
```

### Batch Resolution (SQL)

```sql
-- Mark old rejected events as resolved (e.g., after source system is fixed)
UPDATE inbound_event 
SET resolved = true, resolved_at = NOW()
WHERE source = 'ebuzima/broken-facility' 
  AND status = 'REJECTED'
  AND resolved = false 
  AND received_at < NOW() - INTERVAL '7 days';
```

### Rejection Reasons

| Reason | Description | Action |
|--------|-------------|--------|
| `INVALID_ENVELOPE` | CloudEvents required fields missing/invalid | Fix source system event format |
| `MISSING_REQUIRED_FIELD` | CCE-required field (e.g., subject) missing | Fix source system to include patient UPID |
| `INVALID_SPEC_VERSION` | specversion is not "1.0" | Fix source system |
| `INVALID_FHIR` | FHIR R4 payload cannot be parsed | Fix FHIR resource in source system |
| `KAFKA_PUBLISH_FAILED` | Kafka broker unreachable | Check Kafka cluster health |
| `PROCESSING_ERROR` | Unexpected error during processing | Check application logs |
| `DUPLICATE` | Not rejected — handled as idempotent | N/A |

---

## 4. Database Maintenance

### Table Size Monitoring

```sql
SELECT 
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(oid)) AS total_size,
  pg_size_pretty(pg_relation_size(oid)) AS data_size,
  pg_size_pretty(pg_indexes_size(oid)) AS index_size
FROM pg_class
WHERE relname IN ('inbound_event')
ORDER BY pg_total_relation_size(oid) DESC;
```

### Archive Old Data

```sql
-- Clean old inbound_event records (keep 90 days)
DELETE FROM inbound_event WHERE received_at < NOW() - INTERVAL '90 days';
```

---

## 5. Troubleshooting

### Event Not Reaching Kafka

**Symptoms:** Event returns 500 Internal Server Error.

1. Check `inbound_event` table:
   ```sql
   SELECT id, cloudevents_id, status, rejection_reason, error_details
   FROM inbound_event 
   WHERE cloudevents_id = 'evt-xxx' 
   ORDER BY received_at DESC;
   ```
2. If `status = REJECTED` and `rejection_reason = KAFKA_PUBLISH_FAILURE` → Kafka connectivity issue
3. Check application logs for Kafka errors:
   ```bash
   grep "KafkaPublishException\|kafka.*error\|ProducerFencedException" /var/log/cce-collector.log
   ```
4. Verify Kafka broker health:
   ```bash
   kafka-topics.sh --describe --topic cce.events.inbound --bootstrap-server localhost:9092
   ```

### High Duplicate Rate

**Symptoms:** Many events returning 200 (duplicate) instead of 202 (accepted).

1. Check if source system is retrying:
   ```sql
   SELECT source, cloudevents_id, COUNT(*) 
   FROM inbound_event 
   WHERE status = 'DUPLICATE' 
     AND received_at > NOW() - INTERVAL '1 hour'
   GROUP BY source, cloudevents_id 
   ORDER BY count DESC 
   LIMIT 20;
   ```
2. If the same event IDs keep appearing → source system retry loop, coordinate with source team
3. If different events are being flagged → possible ID collision in source system

### FHIR Validation Failures

**Symptoms:** Events rejected with `INVALID_FHIR` reason.

1. Check rejected events for error details:
   ```sql
   SELECT cloudevents_id, source, rejection_reason, error_details, raw_payload
   FROM inbound_event 
   WHERE status = 'REJECTED' AND rejection_reason = 'INVALID_FHIR' 
   ORDER BY received_at DESC 
   LIMIT 10;
   ```
2. Common causes:
   - Missing `resourceType` field in data payload
   - Malformed JSON in data field
   - Non-standard FHIR resource structure
3. Test locally with HAPI FHIR validator:
   ```bash
   curl -X POST http://localhost:8080/v1/events \
     -H "Content-Type: application/json" \
     -d '{ ... problematic event ... }'
   ```

### Connection Pool Exhaustion

**Symptoms:** Requests timing out, `hikaricp_connections_pending > 0`.

1. Check current pool state:
   ```sql
   SELECT count(*) FROM pg_stat_activity WHERE datname = 'cce_collector';
   ```
2. Identify long-running queries:
   ```sql
   SELECT pid, now() - pg_stat_activity.query_start AS duration, query
   FROM pg_stat_activity
   WHERE datname = 'cce_collector' AND state != 'idle'
   ORDER BY duration DESC;
   ```
3. Increase pool size if needed:
   ```bash
   export SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE=20
   ```

### Application Won't Start

| Error | Cause | Fix |
|-------|-------|-----|
| `Connection refused: localhost:5433` | PostgreSQL not running | Start PostgreSQL |
| `Flyway migration failed` | Schema conflict | Check migration history: `SELECT * FROM flyway_schema_history;` |
| `KafkaException: Node -1 disconnected` | Kafka not reachable | Check Kafka broker |

---

## 6. Log Analysis

### Log Format

The service uses **structured JSON logging** (Logstash encoder). Each log line contains:

```json
{
  "@timestamp": "2025-01-15T09:30:05.123Z",
  "level": "INFO",
  "logger": "o.o.c.c.s.EventIngestionService",
  "message": "Event accepted",
  "correlationId": "corr-abc123",
  "source": "ebuzima/kigali-south",
  "eventId": "evt-001",
  "subject": "patient/UPI-RW-2024-000001"
}
```

### Useful Log Queries (ELK/Loki)

```
# All events from a specific source
logger:"EventIngestionService" AND source:"ebuzima/kigali-south"

# All failed events
level:"ERROR" AND (message:"rejected" OR message:"failed")

# Kafka publish failures
logger:"InboundEventProducer" AND level:"ERROR"

# Trace a specific event
correlationId:"corr-abc123"

# Rejection tracking
logger:"RejectionService" AND message:"rejected"
```

### MDC Fields

The following MDC fields are set per-request for correlation:

| Field | Description |
|-------|-------------|
| `correlationId` | Event correlation ID (generated or from request) |
| `source` | Event source URI |
| `eventId` | CloudEvents `id` |
| `subject` | Patient UPID |

---

## 7. Emergency Procedures

### Kafka Total Outage

1. The service returns **HTTP 500** for all incoming events (Kafka publish fails)
2. Events are persisted in `inbound_event` with `status = 'REJECTED'` and `rejection_reason = 'KAFKA_PUBLISH_FAILURE'`
3. Source systems (openHIM mediators) will retry based on their own retry policy
4. When Kafka recovers, new requests will succeed immediately
5. Previously rejected events can be retried via the rejected event management API:
   ```bash
   # List events rejected due to Kafka failure
   curl "http://localhost:8080/v1/events/rejected?rejectionReason=KAFKA_PUBLISH_FAILURE" | jq .
   # Retry a specific event
   curl -X POST "http://localhost:8080/v1/events/rejected/{id}/retry"
   ```

### Database Total Outage

1. Service returns HTTP 503 on all endpoints
2. No data loss — events are not accepted without DB persistence
3. Source systems (openHIM) will retry based on their retry policy
4. Restore database and restart the service

### Rollback Deployment

```bash
# Kubernetes
kubectl rollout undo deployment/cce-collector-service

# Docker
docker stop cce-collector
docker run -d --name cce-collector [previous-image-tag]
```

### Flyway Migration Rollback

Flyway does not support automatic rollback. For manual rollback:

1. Identify the failed migration: `SELECT * FROM flyway_schema_history ORDER BY installed_rank DESC;`
2. Manually reverse the DDL changes
3. Delete the migration record: `DELETE FROM flyway_schema_history WHERE version = 'X';`
4. Restart the application
