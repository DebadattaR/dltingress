# 05 - Claims Fragment Assembly Pattern

**Related Documents:**
- ← [03-Bronze-Layer.md](03-Bronze-Layer.md) - Bronze Layer Implementation
- ← [04-Silver-Layer.md](04-Silver-Layer.md) - Silver Layer Implementation
- → [06-Gold-Layer.md](06-Gold-Layer.md) - Gold Layer Implementation
- ↑ [00-Index.md](00-Index.md) - Documentation Index

---

## 1. Overview

### 1.1 The Fragment Challenge

In real-world healthcare and claims processing systems, a single business entity (a claim) often arrives in **multiple fragments** at **different times** from upstream systems. This happens because:

- Source systems partition large messages for performance
- Different departments process different claim components
- Network limitations require chunking
- Regulatory requirements separate PHI from financial data

**Example:**
```
Claim CLM-2026-001234 arrives as:
├── Fragment 1 (10:00 AM) → Header, Patient, Provider info
├── Fragment 2 (10:05 AM) → Line items, procedures, diagnoses
└── Fragment 3 (10:15 AM) → Payments, adjustments, final status
```

### 1.2 Solution Architecture

Our pipeline handles this with:

1. **Bronze Layer**: Ingest each fragment independently as it arrives
2. **Fragment Tracking**: Monitor which fragments have been received
3. **Completeness Check**: Identify when all required fragments are present
4. **Silver Layer**: Assemble complete claims from fragments
5. **Monitoring**: Alert on incomplete or overdue claims

---

## 2. Configuration

### 2.1 YAML Configuration

**File**: `config/pipeline_config.yaml`

```yaml
claims_processing:
  fragment_assembly:
    # Maximum wait time for fragments
    max_wait_hours: 24
    
    # Required fragments (must have all)
    required_fragments: ["fragment_1", "fragment_2", "fragment_3"]
    
    # Optional fragments
    optional_fragments: ["fragment_4", "fragment_5"]
    
    # Late-arrival watermark
    late_arrival_watermark_hours: 2
    
    # Assembly strategy
    assembly_strategy: "wait_all"  # or "progressive"

kafka:
  topics:
    claims_fragments:
      - name: "claims-fragment-1"
        partitions: 8
        max_events_per_trigger: 5000
      - name: "claims-fragment-2"
        partitions: 8
        max_events_per_trigger: 5000
      - name: "claims-fragment-3"
        partitions: 8
        max_events_per_trigger: 5000
```

### 2.2 Key Configuration Parameters

| Parameter | Description | Default | Impact |
|-----------|-------------|---------|--------|
| `max_wait_hours` | Max time to wait for all fragments | 24 | Longer = more late arrivals handled |
| `late_arrival_watermark_hours` | Grace period for late data | 2 | Affects when windows close |
| `assembly_strategy` | `wait_all` or `progressive` | `wait_all` | `progressive` = partial assemblies |

---

## 3. Message Format

### 3.1 Fragment 1: Header and Basic Info

```json
{
  "claim_id": "CLM-2026-001234",
  "claim_number": "CLM-2026-001234",
  "fragment_type": "fragment_1",
  "sequence_number": 1,
  "timestamp": "2026-02-08T10:00:00Z",
  "data": {
    "patient_id": "PAT-98765",
    "patient_name": "John Doe",
    "patient_dob": "1985-03-15",
    "provider_id": "PROV-12345",
    "provider_name": "City Medical Center",
    "service_date": "2026-02-05",
    "total_amount": 1500.00,
    "claim_type": "MEDICAL",
    "status": "SUBMITTED"
  },
  "metadata": {
    "source_system": "claims_processing",
    "version": "1.0"
  }
}
```

### 3.2 Fragment 2: Line Items

```json
{
  "claim_id": "CLM-2026-001234",
  "fragment_type": "fragment_2",
  "sequence_number": 2,
  "timestamp": "2026-02-08T10:05:00Z",
  "data": {
    "line_items": [
      {
        "line_number": 1,
        "procedure_code": "99213",
        "procedure_description": "Office Visit - Established Patient",
        "diagnosis_codes": ["Z00.00", "R53.83"],
        "units": 1,
        "charge_amount": 150.00,
        "service_date": "2026-02-05"
      },
      {
        "line_number": 2,
        "procedure_code": "80053",
        "procedure_description": "Comprehensive Metabolic Panel",
        "diagnosis_codes": ["R53.83"],
        "units": 1,
        "charge_amount": 85.00,
        "service_date": "2026-02-05"
      }
    ]
  }
}
```

### 3.3 Fragment 3: Payments and Status

```json
{
  "claim_id": "CLM-2026-001234",
  "fragment_type": "fragment_3",
  "sequence_number": 3,
  "timestamp": "2026-02-08T10:15:00Z",
  "data": {
    "payments": [
      {
        "payment_id": "PAY-2026-001",
        "payment_date": "2026-02-07",
        "payment_amount": 1200.00,
        "payment_method": "ACH"
      }
    ],
    "adjustments": [
      {
        "adjustment_code": "CO-45",
        "adjustment_amount": 300.00,
        "adjustment_reason": "Contractual adjustment"
      }
    ],
    "final_status": "APPROVED",
    "denial_reason": null,
    "adjudication_date": "2026-02-07"
  }
}
```

---

## 4. Data Flow

### 4.1 End-to-End Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEMS                           │
│  Claims Processing → Adjudication → Payment Systems         │
└────────────┬────────────────────┬─────────────┬─────────────┘
             │                    │             │
             ▼                    ▼             ▼
    ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
    │ Fragment 1     │  │ Fragment 2     │  │ Fragment 3     │
    │ Topic          │  │ Topic          │  │ Topic          │
    └────────┬───────┘  └────────┬───────┘  └────────┬───────┘
             │                    │                    │
             └────────────────────┴────────────────────┘
                                  │
                                  ▼
                    ┌──────────────────────────────┐
                    │  BRONZE LAYER                │
                    │  - bronze_claims_fragment_1  │
                    │  - bronze_claims_fragment_2  │
                    │  - bronze_claims_fragment_3  │
                    │  - fragment_tracking         │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │  FRAGMENT COMPLETENESS       │
                    │  - Check all fragments       │
                    │  - Apply watermark           │
                    │  - Track wait times          │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────┴───────────────┐
                    │                              │
                    ▼                              ▼
        ┌───────────────────┐        ┌──────────────────────┐
        │  COMPLETE CLAIMS  │        │  INCOMPLETE CLAIMS   │
        │  (All fragments)  │        │  (Missing fragments) │
        └─────────┬─────────┘        └──────────────────────┘
                  │
                  ▼
        ┌──────────────────────────────┐
        │  SILVER LAYER                │
        │  - silver_claims_assembled   │
        │  - Joined all fragments      │
        │  - Data quality validated    │
        └──────────────┬───────────────┘
                       │
                       ▼
            ┌─────────────────────┐
            │  GOLD LAYER         │
            │  - Aggregations     │
            │  - Analytics        │
            └─────────────────────┘
```

### 4.2 Timing Example

```
Time    Event                                      State
-----   ----------------------------------------   -------------------------
10:00   Fragment 1 arrives for CLM-001234         Incomplete (1/3)
10:05   Fragment 2 arrives for CLM-001234         Incomplete (2/3)
10:08   Fragment 1 arrives for CLM-001235         Incomplete (1/3)
10:15   Fragment 3 arrives for CLM-001234         COMPLETE ✓ → Silver
10:20   Fragment 2 arrives for CLM-001235         Incomplete (2/3)
12:00   24 hours since CLM-001235 fragment 1      Alert: Overdue
```

---

## 5. Implementation Details

### 5.1 Bronze Layer - Ingestion

**Key Features:**
- Separate Kafka topic per fragment type
- Independent streaming ingestion
- Minimal transformation (parse JSON only)
- Append-only storage
- Fragment tracking table

**Code**: `pipelines/bronze_claims_fragments.py`

**Tables Created:**
- `bronze_claims_fragment_1_raw` - Raw binary from Kafka
- `bronze_claims_fragment_1_parsed` - JSON parsed
- `bronze_claims_fragment_2_raw`
- `bronze_claims_fragment_2_parsed`
- `bronze_claims_fragment_3_raw`
- `bronze_claims_fragment_3_parsed`
- `bronze_claims_fragment_tracking` - Monitor arrivals

### 5.2 Silver Layer - Assembly

**Key Features:**
- Watermarking for late data
- Windowed aggregation to collect fragments
- Completeness checking
- Stream-stream joins
- Quality expectations

**Code**: `pipelines/silver_claims_assembly.py`

**Tables Created:**
- `silver_claims_fragment_completeness` - Which claims are complete
- `silver_claims_assembled` - Final complete claims
- `silver_claims_incomplete` - Missing fragments (monitoring)
- `silver_claims_quarantine` - Failed quality checks

### 5.3 Watermarking Strategy

```python
# Allow 2 hours for late-arriving fragments
.withWatermark("fragment_received_at", "2 hours")

# Group into 24-hour windows
.groupBy(
    "claim_id",
    window(col("fragment_received_at"), "24 hours")
)
```

**How it works:**
1. Spark tracks event time from `fragment_received_at`
2. Watermark = 2 hours behind latest event seen
3. Events older than watermark are dropped (too late)
4. Windows remain open until watermark passes window end

---

## 6. Monitoring & Alerts

### 6.1 Key Metrics to Monitor

```sql
-- Claims waiting for fragments
SELECT
    COUNT(DISTINCT claim_id) AS incomplete_claims,
    AVG(wait_time_hours) AS avg_wait_hours,
    MAX(wait_time_hours) AS max_wait_hours
FROM dev_data_platform.silver_cleansed.silver_claims_incomplete
WHERE silver_processed_at >= current_timestamp() - INTERVAL 1 HOUR;

-- Claims assembled in last hour
SELECT
    COUNT(*) AS assembled_count,
    AVG(line_item_count) AS avg_lines_per_claim,
    SUM(total_amount) AS total_billed
FROM dev_data_platform.silver_cleansed.silver_claims_assembled
WHERE assembly_timestamp >= current_timestamp() - INTERVAL 1 HOUR;

-- Overdue claims (> 24 hours waiting)
SELECT
    claim_id,
    received_fragments,
    missing_fragments,
    wait_time_hours
FROM dev_data_platform.silver_cleansed.silver_claims_incomplete
WHERE wait_time_hours > 24
ORDER BY wait_time_hours DESC;
```

### 6.2 Alerting Rules

| Condition | Severity | Action |
|-----------|----------|--------|
| Claim waiting > 24 hours | High | Email + Slack |
| >100 incomplete claims | Medium | Slack notification |
| Fragment drop rate > 5% | Critical | Page on-call |
| No fragments in 1 hour | Critical | Page on-call |

### 6.3 Dashboard Queries

```sql
-- Fragment arrival pattern (hourly)
SELECT
    DATE_TRUNC('hour', fragment_received_at) AS hour,
    fragment_type,
    COUNT(*) AS fragment_count
FROM dev_data_platform.bronze_raw.bronze_claims_fragment_tracking
WHERE fragment_received_at >= current_date()
GROUP BY hour, fragment_type
ORDER BY hour DESC;

-- Assembly metrics
SELECT
    metric_hour,
    claim_status,
    claim_count,
    total_billed,
    total_paid
FROM dev_data_platform.silver_cleansed.silver_claims_metrics
WHERE metric_hour >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY metric_hour DESC;
```

---

## 7. Troubleshooting

### 7.1 Missing Fragments

**Symptom:** Claims stuck in incomplete state

**Diagnosis:**
```sql
-- Find claims with missing fragments
SELECT
    claim_id,
    received_fragments,
    missing_fragments,
    first_fragment_received,
    wait_time_hours
FROM dev_data_platform.silver_cleansed.silver_claims_incomplete
WHERE is_overdue = true
ORDER BY wait_time_hours DESC
LIMIT 20;
```

**Resolution:**
1. Check source system - is it sending all fragments?
2. Check Kafka topic lag - are messages backed up?
3. Verify topic naming - correct topic routing?
4. Check for errors in Bronze ingestion pipeline

### 7.2 Duplicate Fragments

**Symptom:** Same fragment arriving multiple times

**Diagnosis:**
```sql
-- Check for duplicate fragments
SELECT
    claim_id,
    fragment_type,
    COUNT(*) AS duplicate_count
FROM dev_data_platform.bronze_raw.bronze_claims_fragment_tracking
GROUP BY claim_id, fragment_type
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

**Resolution:**
- Already handled by deduplication in Silver layer
- Uses `row_number()` to keep latest version
- No action needed unless duplicates are excessive

### 7.3 Assembly Performance Issues

**Symptom:** Long processing times, high lag

**Diagnosis:**
```sql
-- Check window sizes
SELECT
    window_start,
    window_end,
    COUNT(DISTINCT claim_id) AS claims_in_window
FROM dev_data_platform.silver_cleansed.silver_claims_fragment_completeness
GROUP BY window_start, window_end
ORDER BY claims_in_window DESC;
```

**Resolution:**
1. Increase cluster size (more workers)
2. Reduce window duration if too many claims per window
3. Increase maxEventsPerTrigger in Bronze
4. Enable Photon engine

---

## 8. Testing

### 8.1 Send Test Fragments

**Using the utility:**

```python
from utils.kafka_utils import ConfigManager, KafkaUtility, ClaimsFragmentUtility

# Initialize
config_manager = ConfigManager("config/pipeline_config.yaml", "dev")
kafka_util = KafkaUtility(config_manager)
claims_util = ClaimsFragmentUtility(config_manager)

# Create producer
producer = kafka_util.create_producer()

# Send fragment 1
fragment_1_msg = claims_util.create_fragment_message(
    claim_id="TEST-CLM-001",
    fragment_type="fragment_1",
    fragment_data={
        "claim_number": "TEST-CLM-001",
        "patient_id": "TEST-PAT-001",
        "provider_id": "TEST-PROV-001",
        "service_date": "2026-02-08",
        "total_amount": 1000.00,
        "status": "SUBMITTED"
    }
)

kafka_util.send_message(
    producer=producer,
    topic="claims-fragment-1",
    key="TEST-CLM-001",
    value=fragment_1_msg
)

# Send fragments 2 and 3 similarly...
```

### 8.2 Verify Assembly

```sql
-- Check if test claim assembled
SELECT *
FROM dev_data_platform.silver_cleansed.silver_claims_assembled
WHERE claim_id = 'TEST-CLM-001';

-- Check completeness
SELECT *
FROM dev_data_platform.silver_cleansed.silver_claims_fragment_completeness
WHERE claim_id = 'TEST-CLM-001';
```

---

## 9. Best Practices

### ✅ Do's

- **Use consistent claim_id** across all fragments
- **Include sequence_number** for ordering
- **Add timestamp** to every fragment
- **Monitor incomplete claims** daily
- **Set appropriate watermarks** (2-4 hours typical)
- **Test with incomplete scenarios** (missing fragments)

### ❌ Don'ts

- **Don't use very long windows** (>48 hours) - memory issues
- **Don't skip sequence numbers** - helps debug ordering
- **Don't ignore overdue alerts** - indicates upstream issues
- **Don't manually modify Bronze** - breaks stream processing
- **Don't set watermark too short** - drops valid late data

---

## 10. Advanced Patterns

### 10.1 Progressive Assembly

Assemble partial claims as fragments arrive:

```python
# Instead of waiting for all fragments
assembly_strategy: "progressive"

# Assembly logic
.withColumn("partial_complete",
    # Fragment 1 + 2 is enough for basic reporting
    when(
        array_contains(col("received_fragments"), "fragment_1") &
        array_contains(col("received_fragments"), "fragment_2"),
        True
    ).otherwise(False)
)
```

### 10.2 Fragment Versioning

Handle updated fragments:

```python
# Keep latest version based on sequence_number
.withColumn("row_num",
    row_number().over(
        Window.partitionBy("claim_id", "fragment_type")
              .orderBy(col("sequence_number").desc())
    )
)
.filter(col("row_num") == 1)
```

---

**References:**
- [Bronze Layer Code](../pipelines/bronze_claims_fragments.py)
- [Silver Layer Code](../pipelines/silver_claims_assembly.py)
- [Kafka Utilities](../utils/kafka_utils.py)
- [Configuration](../config/pipeline_config.yaml)
