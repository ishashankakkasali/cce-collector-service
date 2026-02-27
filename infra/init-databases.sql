-- ============================================================
-- Shared PostgreSQL initialization for CCE platform services.
-- Runs once when the container volume is first created.
-- ============================================================

-- Collector Service database & role
CREATE USER cce_collector WITH PASSWORD 'cce_collector';
CREATE DATABASE cce_collector OWNER cce_collector;
GRANT ALL PRIVILEGES ON DATABASE cce_collector TO cce_collector;

-- Compliance Service database & role
CREATE USER cce_compliance WITH PASSWORD 'cce_compliance';
CREATE DATABASE cce OWNER cce_compliance;
GRANT ALL PRIVILEGES ON DATABASE cce TO cce_compliance;
