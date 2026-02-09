# 01 - Architecture Overview

**Related Documents:**
- ← [00-Index.md](00-Index.md) - Documentation Index
- → [02-Infrastructure-Setup.md](02-Infrastructure-Setup.md) - Infrastructure Setup

---

## 1. System Architecture

### 1.1 High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
├──────────────────┬────────────────────┬─────────────────────────┤
│  Azure Event     │   REST APIs        │   SFTP Server           │
│  Hubs / Kafka    │   (External)       │   (Partner Data)        │
│  (90% volume)    │   (~5% volume)     │   (~5% volume)          │
└────────┬─────────┴──────────┬─────────┴────────────┬────────────┘
         │                    │                      │
         │ Streaming          │ API Orchestration    │ File Transfer
         │                    │ (Azure Data Factory) │ (ADF/Logic App)
         │                    │                      │
         │                    ▼                      ▼
         │              ┌──────────────────────────────────┐
         │              │   ADLS Gen2 Landing Zones        │
         │              │   - /api-landing/                │
         │              │   - /sftp-landing/               │
         │              └──────────┬───────────────────────┘
         │                         │
         │                         │ AutoLoader
         ▼                         ▼
┌────────────────────────────────────────────────────────────────┐
│               AZURE DATABRICKS + UNITY CATALOG                 │
│                                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │                  LAKEFLOW PIPELINES                      │ │
│  │                                                          │ │
│  │  ┌────────────────────────────────────────────────┐     │ │
│  │  │  BRONZE (Raw)                                  │     │ │
│  │  │  - bronze_raw.kafka_events_raw                 │     │ │
│  │  │  - bronze_raw.api_data_raw                     │     │ │
│  │  │  - bronze_raw.sftp_files_raw                   │     │ │
│  │  └───────────────────┬────────────────────────────┘     │ │
│  │                      │                                   │ │
│  │  ┌───────────────────▼────────────────────────────┐     │ │
│  │  │  SILVER (Cleansed)                             │     │ │
│  │  │  - Data quality expectations                   │     │ │
│  │  │  - Deduplication                               │     │ │
│  │  │  - Type standardization                        │     │ │
│  │  │  - SCD Type 2 (where applicable)               │     │ │
│  │  └───────────────────┬────────────────────────────┘     │ │
│  │                      │                                   │ │
│  │  ┌───────────────────▼────────────────────────────┐     │ │
│  │  │  GOLD (Business)                               │     │ │
│  │  │  - Aggregations and KPIs                       │     │ │
│  │  │  - Dimensional models                          │     │ │
│  │  │  - Optimized for analytics                     │     │ │
│  │  └────────────────────────────────────────────────┘     │ │
│  │                                                          │ │
│  └──────────────────────────────────────────────────────────┘ │
│                                                                │
│  Storage: ADLS Gen2 (/data/bronze/, /data/silver/, /data/gold/)│
└────────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────┐
                    │    CONSUMPTION LAYER       │
                    ├────────────────────────────┤
                    │  - Power BI                │
                    │  - Azure Synapse Analytics │
                    │  - Databricks SQL          │
                    │  - ML Workloads            │
                    └────────────────────────────┘
```

---

## 2. Azure Components

### 2.1 Core Services

| Service | Purpose | Configuration |
|---------|---------|---------------|
| **Azure Databricks** | Compute platform for Lakeflow pipelines | Premium tier with Unity Catalog |
| **ADLS Gen2** | Data lake storage for all layers | Hierarchical namespace enabled |
| **Unity Catalog** | Centralized governance and metadata | Regional metastore |
| **Azure Event Hubs** | Kafka-compatible streaming ingestion | Standard tier, auto-inflate enabled |
| **Azure Key Vault** | Secrets and credential management | RBAC enabled |
| **Azure Monitor** | Observability and logging | Log Analytics workspace |
| **Azure Data Factory** | Orchestration for API/SFTP ingestion | Integration runtime |
| **Azure DevOps** | CI/CD and source control | Git repositories, pipelines |

### 2.2 Network Architecture

```
┌────────────────────────────────────────────────────────┐
│              Azure Virtual Network (VNet)              │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Databricks Workspace (VNet Injection)           │ │
│  │  - Public subnet (control plane)                 │ │
│  │  - Private subnet (data plane)                   │ │
│  │  - NSG rules for secure access                   │ │
│  └──────────────────────────────────────────────────┘ │
│                                                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Private Endpoints                               │ │
│  │  - ADLS Gen2                                     │ │
│  │  - Azure Key Vault                               │ │
│  │  - Event Hubs (optional)                         │ │
│  └──────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

**Security Features:**
- No public internet access to data storage
- Private endpoints for all Azure services
- Managed identities for authentication
- Network isolation for Databricks clusters

---

## 3. Unity Catalog Design

### 3.1 Three-Level Namespace

```
<catalog>.<schema>.<table/view>

Example:
prod_data_platform.silver_cleansed.events_clean
```

### 3.2 Catalog Structure by Environment

**Development Environment:**
```
dev_data_platform (catalog)
├── bronze_raw (schema)
├── silver_cleansed (schema)
└── gold_analytics (schema)
```

**Test/QA Environment:**
```
test_data_platform (catalog)
├── bronze_raw (schema)
├── silver_cleansed (schema)
└── gold_analytics (schema)
```

**Production Environment:**
```
prod_data_platform (catalog)
├── bronze_raw (schema)
├── silver_cleansed (schema)
└── gold_analytics (schema)
```

### 3.3 Schema Purposes

| Schema | Purpose | Table Types | Retention |
|--------|---------|-------------|-----------|
| **bronze_raw** | Raw data ingestion, no transformations | Streaming tables, append-only | 30-90 days |
| **silver_cleansed** | Cleansed, deduplicated, validated | SCD Type 2, materialized views | 1-2 years |
| **gold_analytics** | Business aggregations and KPIs | Fact/dimension tables, views | Long-term (3-5 years) |

---

## 4. Data Flow Patterns

### 4.1 Kafka/Event Hubs Streaming (90%)

```python
# Simplified flow - full code in 03-Bronze-Layer.md

Event Hubs → Lakeflow Streaming Table (Bronze) 
          → Lakeflow Transformation (Silver) 
          → Lakeflow Aggregation (Gold)

Processing mode: Continuous (always-on)
Latency: Near real-time (seconds to minutes)
```

**Use Cases:**
- Customer clickstream events
- Transaction processing
- Application logs
- IoT sensor data

### 4.2 REST API Batch (~5%)

```python
# Simplified flow - full code in 03-Bronze-Layer.md

External API → Azure Data Factory → ADLS Landing Zone 
            → Lakeflow AutoLoader (Bronze) 
            → Lakeflow Transformation (Silver) 
            → Lakeflow Aggregation (Gold)

Processing mode: Triggered (scheduled or event-driven)
Latency: Minutes to hours (based on schedule)
```

**Use Cases:**
- Third-party enrichment data
- Reference data updates
- Partner API integrations

### 4.3 SFTP File Transfer (~5%)

```python
# Simplified flow - full code in 03-Bronze-Layer.md

SFTP Server → Azure Data Factory → ADLS Landing Zone 
           → Lakeflow AutoLoader (Bronze) 
           → Lakeflow Transformation (Silver) 
           → Lakeflow Aggregation (Gold)

Processing mode: Triggered (scheduled daily/hourly)
Latency: Hours (based on file availability)
```

**Use Cases:**
- Daily reconciliation files
- Legacy system exports
- Partner data feeds

---

## 5. Lakeflow Pipeline Architecture

### 5.1 Pipeline Definition

Lakeflow pipelines are defined in Python notebooks using decorators:

```python
import dlt
from pyspark.sql.functions import *

# Bronze table - raw ingestion
@dlt.table(
    name="bronze_kafka_events",
    comment="Raw events from Event Hubs",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "event_timestamp"
    }
)
def bronze_kafka_events():
    return (
        spark.readStream
            .format("eventhubs")
            .options(**kafka_config)
            .load()
    )

# Silver table - cleansed data
@dlt.table(
    name="silver_events_clean",
    comment="Cleansed and validated events"
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
def silver_events_clean():
    return (
        dlt.read_stream("bronze_kafka_events")
            .select(/* transformations */)
    )
```

### 5.2 Pipeline Types

**Continuous Pipelines (Kafka/Event Hubs):**
- Always running
- Process new data immediately
- Use checkpointing for fault tolerance
- Ideal for streaming sources

**Triggered Pipelines (API/SFTP):**
- Run on schedule or event
- Process all new data since last run
- More cost-effective for batch sources
- Can be combined in same pipeline

---

## 6. Data Quality Strategy

### 6.1 Expectations Framework

Lakeflow uses three levels of data quality expectations:

```python
# WARN - Log violations but continue
@dlt.expect("valid_amount", "amount >= 0")

# DROP - Drop rows that violate
@dlt.expect_or_drop("valid_email", "email IS NOT NULL")

# FAIL - Stop pipeline on violation
@dlt.expect_or_fail("critical_field", "id IS NOT NULL")
```

### 6.2 Quality Gates by Layer

| Layer | Quality Level | Approach |
|-------|---------------|----------|
| **Bronze** | Minimal | Accept all data, log schema issues |
| **Silver** | Strict | Drop invalid records, enforce business rules |
| **Gold** | Critical | Fail on data quality issues that affect KPIs |

---

## 7. Performance Considerations

### 7.1 Partitioning Strategy

```
Bronze Layer:   Partition by ingestion date
                /data/bronze/kafka_events/year=2026/month=02/day=08/

Silver Layer:   Partition by business date
                /data/silver/events_clean/event_date=2026-02-08/

Gold Layer:     Partition by aggregation period
                /data/gold/daily_metrics/metric_date=2026-02-08/
```

### 7.2 Optimization Techniques

- **Z-Ordering:** On frequently filtered columns (event_id, customer_id)
- **Auto-optimize:** Enabled for automatic file compaction
- **Liquid Clustering:** For tables with high cardinality columns (coming soon)
- **Photon Engine:** Enabled for faster query performance

---

## 8. Security Architecture

### 8.1 Authentication & Authorization

```
Azure AD Identity
      ↓
Unity Catalog RBAC
      ↓
Catalog-level permissions
      ↓
Schema-level permissions
      ↓
Table-level permissions
```

**Permission Model:**
- **Data Engineers:** Read/Write on dev catalogs, Read on prod
- **Analytics Team:** Read on gold schemas
- **Admin:** Full control across all catalogs

### 8.2 Data Protection

- **Encryption at rest:** Azure Storage Service Encryption (SSE)
- **Encryption in transit:** TLS 1.2+
- **Column-level security:** Dynamic views for PII masking
- **Row-level security:** WHERE clause filters in Unity Catalog

---

## 9. Monitoring & Observability

### 9.1 Lakeflow Pipeline Metrics

Built-in metrics available in Databricks:
- Pipeline run status (success/failure)
- Data quality expectation violations
- Processing lag for streaming tables
- Row counts per table
- Lineage graph

### 9.2 Azure Monitor Integration

```
Databricks Logs → Azure Monitor → Log Analytics
                                        ↓
                            Alerts & Dashboards
```

**Key Metrics to Monitor:**
- Pipeline execution duration
- Data freshness (time since last update)
- Error rates and failure patterns
- Cost per pipeline run

---

## 10. Disaster Recovery

### 10.1 Backup Strategy

- **Data:** ADLS Gen2 with geo-redundant storage (GRS)
- **Metadata:** Unity Catalog metastore backup
- **Code:** Git repository with branching strategy
- **Configuration:** Infrastructure as Code (Terraform)

### 10.2 Recovery Objectives

- **RPO (Recovery Point Objective):** < 1 hour for critical data
- **RTO (Recovery Time Objective):** < 4 hours for full pipeline restoration

### 10.3 Delta Lake Time Travel

```sql
-- Query data from 24 hours ago
SELECT * FROM prod_data_platform.silver_cleansed.events_clean
VERSION AS OF 24 HOURS AGO

-- Restore table to previous version
RESTORE TABLE prod_data_platform.silver_cleansed.events_clean
TO VERSION AS OF 100
```

---

## 11. Scalability Design

### 11.1 Horizontal Scaling

- **Auto-scaling clusters:** 2-20 workers based on load
- **Partition pruning:** Efficient data skipping
- **Broadcast joins:** For small dimension tables (<10 GB)

### 11.2 Vertical Scaling

- **Node types:** 
  - Development: Standard_DS3_v2 (4 cores, 14 GB)
  - Production streaming: Standard_E8s_v3 (8 cores, 64 GB memory-optimized)
  - Production batch: Standard_D8s_v3 (8 cores, 32 GB general purpose)

### 11.3 Cost Optimization

- **Spot instances:** For non-critical batch workloads (up to 80% savings)
- **Cluster auto-termination:** After 30 minutes of inactivity
- **Photon engine:** 2-3x performance at similar cost
- **Serverless compute:** For SQL analytics workloads

---

## 12. Development Workflow

### 12.1 Environment Promotion

```
Developer Workspace (personal)
          ↓
Dev Catalog (shared development)
          ↓
Test Catalog (integration testing)
          ↓
Prod Catalog (production)
```

### 12.2 Git Workflow

```
feature/bronze-kafka-ingestion
          ↓
develop (integration branch)
          ↓
release/v1.0.0 (testing)
          ↓
main (production)
```

### 12.3 Testing Strategy

- **Unit tests:** PySpark test utilities for transformations
- **Integration tests:** End-to-end pipeline runs on test data
- **Data quality tests:** Validate expectations and row counts
- **Performance tests:** Benchmark query response times

---

## 13. Next Steps

After understanding this architecture overview, proceed to:

1. **[02-Infrastructure-Setup.md](02-Infrastructure-Setup.md)** - Set up Azure resources and Unity Catalog
2. **[03-Bronze-Layer.md](03-Bronze-Layer.md)** - Implement raw data ingestion
3. **[04-Silver-Layer.md](04-Silver-Layer.md)** - Build data cleansing pipelines
4. **[05-Claims-Fragment-Assembly.md](05-Claims-Fragment-Assembly.md)** - Assemble claims fragments (if applicable)
5. **[06-Gold-Layer.md](06-Gold-Layer.md)** - Create analytics-ready datasets

---

## 14. Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| Feb 8, 2026 | Use Azure Event Hubs instead of managed Kafka | Better Azure integration, lower ops overhead |
| Feb 8, 2026 | Unity Catalog with 3 environment catalogs | Clear isolation, easier promotion |
| Feb 8, 2026 | Medallion architecture (Bronze/Silver/Gold) | Industry standard, clear data lineage |
| Feb 8, 2026 | Lakeflow for all ETL workloads | Declarative, built-in quality, UC integration |

---

**References:**
- ← [00-Index.md](00-Index.md)
- → [02-Infrastructure-Setup.md](02-Infrastructure-Setup.md)
