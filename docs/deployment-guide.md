# CCE Collector Service — Deployment Guide

## 1. Prerequisites

| Dependency | Minimum Version | Notes |
|-----------|----------------|-------|
| Java | 21 (LTS) | Eclipse Temurin recommended |
| Maven | 3.9+ | Only for building from source |
| PostgreSQL | 16+ | Primary datastore |
| Apache Kafka | 3.7+ | KRaft mode (no ZooKeeper) |
| Docker | 24+ | For containerized deployment |
| Docker Compose | 2.20+ | For local development |

---

## 2. Local Development Setup

### 2.1 Start Infrastructure

```bash
cd /path/to/cce-collector-service-jp
docker compose up -d
```

This starts:
- **PostgreSQL** on port `5433` (user: `cce_user`, password: `cce_pass`, database: `cce_collector`)
- **Kafka** on port `9092` (KRaft mode, single broker)

### 2.2 Build the Application

```bash
mvn clean package -DskipTests
```

### 2.3 Run with Local Profile

```bash
java -jar target/cce-collector-service-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=local
```

Or using Maven:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

The application will start on port **8080**.

### 2.4 Verify

```bash
# Health check
curl http://localhost:8080/actuator/health

# Submit a test event
curl -X POST http://localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "specversion": "1.0",
    "id": "test-event-001",
    "source": "test/local",
    "type": "cce.encounter.created",
    "subject": "patient/TEST-001",
    "data": { "resourceType": "Encounter", "status": "finished" }
  }'
```

---

## 3. Configuration Profiles

| Profile | File | Description |
|---------|------|-------------|
| (default) | `application.yml` | Base configuration |
| `local` | `application-local.yml` | Debug logging, relaxed pool sizes |
| `staging` | `application-staging.yml` | Moderate pool sizes |
| `production` | `application-production.yml` | Optimized pool sizes, shorter retry intervals |

Activate a profile:
```bash
java -jar target/cce-collector-service-1.0.0-SNAPSHOT.jar \
  --spring.profiles.active=production
```

---

## 4. Environment Variables

All configuration can be overridden via environment variables using Spring Boot's relaxed binding:

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5433/cce_collector` | JDBC URL |
| `SPRING_DATASOURCE_USERNAME` | `cce_user` | Database username |
| `SPRING_DATASOURCE_PASSWORD` | `cce_pass` | Database password |
| `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE` | `10` | Connection pool max size |
| `SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE` | `5` | Connection pool min idle |

### Kafka

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker(s) |
| `SPRING_KAFKA_PRODUCER_ACKS` | `all` | Producer acknowledgement level |
| `SPRING_KAFKA_PRODUCER_RETRIES` | `3` | Producer retry count |

### Application-Specific

| Variable | Default | Description |
|----------|---------|-------------|
| `CCE_COLLECTOR_FHIR_STRICT_VALIDATION` | `false` | Enable strict FHIR validation |
| `CCE_COLLECTOR_DEDUP_LOOKBACK_DAYS` | `30` | Dedup lookback window (days) |
| `CCE_COLLECTOR_KAFKA_TOPICS_INBOUND` | `cce.events.inbound` | Inbound events topic |
| `CCE_COLLECTOR_KAFKA_TOPICS_DEAD_LETTER` | `cce.deadletter` | Rejected event monitoring topic (optional) |

---

## 5. Docker Build

### Build Image

```bash
docker build -t cce-collector-service:latest .
```

The Dockerfile uses a **multi-stage build**:
1. Stage 1 (`builder`): `eclipse-temurin:21-jdk-alpine` — compiles `mvn package`
2. Stage 2 (`runtime`): `eclipse-temurin:21-jre-alpine` — minimal runtime image

### Run Container

```bash
docker run -d \
  --name cce-collector \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=production \
  -e SPRING_DATASOURCE_URL=jdbc:postgresql://db-host:5432/cce_collector \
  -e SPRING_DATASOURCE_USERNAME=cce_user \
  -e SPRING_DATASOURCE_PASSWORD=cce_pass \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
  cce-collector-service:latest
```

---

## 6. Database Migrations

Flyway runs automatically on startup. Migrations are located in `src/main/resources/db/migration/`:

| Migration | Description |
|-----------|-------------|
| `V1__create_inbound_event.sql` | `inbound_event` table with dedup constraint and rejection tracking columns |

### Manual Migration Execution

```bash
mvn flyway:migrate \
  -Dflyway.url=jdbc:postgresql://localhost:5433/cce_collector \
  -Dflyway.user=cce_user \
  -Dflyway.password=cce_pass
```

---

## 7. Kubernetes Deployment

### Health Probes

Configure Kubernetes liveness and readiness probes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cce-collector-service
  labels:
    app: cce-collector
spec:
  replicas: 2
  selector:
    matchLabels:
      app: cce-collector
  template:
    metadata:
      labels:
        app: cce-collector
    spec:
      containers:
        - name: cce-collector
          image: cce-collector-service:latest
          ports:
            - containerPort: 8080
          env:
            - name: SPRING_PROFILES_ACTIVE
              value: production
            - name: SPRING_DATASOURCE_URL
              valueFrom:
                secretKeyRef:
                  name: cce-db-secret
                  key: url
            - name: SPRING_DATASOURCE_USERNAME
              valueFrom:
                secretKeyRef:
                  name: cce-db-secret
                  key: username
            - name: SPRING_DATASOURCE_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: cce-db-secret
                  key: password
            - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: cce-kafka-config
                  key: bootstrap-servers
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8080
            initialDelaySeconds: 15
            periodSeconds: 5
            failureThreshold: 3
          resources:
            requests:
              cpu: 500m
              memory: 512Mi
            limits:
              cpu: "1"
              memory: 1Gi
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: cce-collector-service
spec:
  selector:
    app: cce-collector
  ports:
    - port: 80
      targetPort: 8080
  type: ClusterIP
```

### Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cce-db-secret
type: Opaque
stringData:
  url: jdbc:postgresql://pg-host:5432/cce_collector
  username: cce_user
  password: <your-password>
```

---

## 8. Kafka Topic Setup

If auto-creation is disabled, create topics manually:

```bash
# Inbound events topic (12 partitions, replication factor 3)
kafka-topics.sh --create \
  --topic cce.events.inbound \
  --partitions 12 \
  --replication-factor 3 \
  --bootstrap-server kafka:9092

# Rejected event monitoring topic (optional, 3 partitions, replication factor 3)
kafka-topics.sh --create \
  --topic cce.deadletter \
  --partitions 3 \
  --replication-factor 3 \
  --bootstrap-server kafka:9092
```

### Recommended Topic Configuration

| Setting | Value | Rationale |
|---------|-------|-----------|
| `retention.ms` | `604800000` (7 days) | Sufficient replay window |
| `min.insync.replicas` | `2` | HA with `acks=all` |
| `cleanup.policy` | `delete` | Event stream, not compacted |

---

## 9. Production Checklist

- [ ] PostgreSQL: Create database `cce_collector`, grant permissions to `cce_user`
- [ ] PostgreSQL: Tune `shared_buffers`, `work_mem`, `effective_cache_size`
- [ ] Kafka: Create topics with production replication factor (≥ 3)
- [ ] Kafka: Set `min.insync.replicas=2` on inbound topic
- [ ] Application: Set `SPRING_PROFILES_ACTIVE=production`
- [ ] Application: Set real database credentials via secrets
- [ ] Monitoring: Expose `/actuator/prometheus` to Prometheus scraper
- [ ] Logging: Configure log aggregation (ELK/Loki) — JSON structured output enabled by default
- [ ] TLS: Terminate TLS at load balancer or ingress controller
- [ ] Backups: Schedule PostgreSQL pg_dump for `inbound_event`
