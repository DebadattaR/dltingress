# 10 - Operations Runbook

**Related Documents:**
- ← [09-Deployment-CICD.md](09-Deployment-CICD.md) - Deployment & CI/CD
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview
- ↑ [00-Index.md](00-Index.md) - Documentation Index

---

## 1. Daily Operations

### 1.1 Morning Health Check

**Run daily at 9 AM:**

```sql
-- 1. Check pipeline status
SELECT
    details:pipeline_name AS pipeline_name,
    MAX(timestamp) AS last_run,
    COUNT(CASE WHEN event_type = 'flow_failed' THEN 1 END) AS failures_last_24h
FROM event_log('<pipeline-id>')
WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY details:pipeline_name;

-- 2. Check data freshness
SELECT
    'Bronze' AS layer,
    MAX(ingestion_timestamp) AS last_update,
    TIMESTAMPDIFF(HOUR, MAX(ingestion_timestamp), CURRENT_TIMESTAMP()) AS hours_old
FROM dev_data_platform.bronze_raw.bronze_kafka_events_raw

UNION ALL

SELECT
    'Silver',
    MAX(silver_processed_timestamp),
    TIMESTAMPDIFF(HOUR, MAX(silver_processed_timestamp), CURRENT_TIMESTAMP())
FROM dev_data_platform.silver_cleansed.silver_events_clean;

-- 3. Check data quality metrics
SELECT
    table_name,
    SUM(dropped_records) AS total_dropped,
    AVG(drop_rate_pct) AS avg_drop_rate
FROM monitoring.data_quality_metrics
WHERE metric_date = current_date() - INTERVAL 1 DAY
GROUP BY table_name
HAVING avg_drop_rate > 2;
```

### 1.2 Resource Utilization Check

```bash
# Check cluster utilization via Azure CLI
az databricks workspace show \
  --resource-group rg-data-platform-prod \
  --name dbw-data-platform-prod

# Or use Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
clusters = w.clusters.list()

for cluster in clusters:
    print(f"Cluster: {cluster.cluster_name}")
    print(f"State: {cluster.state}")
    print(f"Workers: {cluster.num_workers}")
```

---

## 2. Common Issues & Resolutions

### 2.1 Pipeline Failures

#### Issue: Event Hubs Connection Timeout

**Symptoms:**
- Pipeline fails with "Connection timeout to Event Hubs"
- Event logs show network errors

**Diagnosis:**
```python
# Test Event Hubs connectivity
import requests

eventhub_namespace = "evhns-data-platform-prod"
url = f"https://{eventhub_namespace}.servicebus.windows.net"

try:
    response = requests.get(url, timeout=5)
    print(f"Status: {response.status_code}")
except Exception as e:
    print(f"Connection failed: {e}")
```

**Resolution:**
1. Check Event Hubs service health in Azure Portal
2. Verify network security group (NSG) rules
3. Confirm connection string in Key Vault is valid
4. Restart the DLT pipeline

```bash
# Restart pipeline via CLI
databricks pipelines start-update --pipeline-id <pipeline-id>
```

---

#### Issue: Out of Memory Errors

**Symptoms:**
- Executors failing with "OutOfMemoryError"
- Pipeline runs slowly or hangs

**Diagnosis:**
```sql
-- Check for data skew
SELECT
    kafka_partition,
    COUNT(*) AS record_count
FROM dev_data_platform.bronze_raw.bronze_kafka_events_raw
WHERE ingestion_date = current_date()
GROUP BY kafka_partition
ORDER BY record_count DESC;
```

**Resolution:**
1. **Immediate:** Increase cluster size
   ```python
   # Update cluster config in DLT pipeline settings
   # Increase max_workers from 8 to 16
   ```

2. **Long-term:** Optimize partitioning
   ```python
   # Repartition before expensive operations
   .repartition(200, "event_id")
   ```

3. Enable Adaptive Query Execution (AQE)
   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   ```

---

#### Issue: Schema Evolution Failures

**Symptoms:**
- AutoLoader fails with "Schema mismatch"
- New columns not appearing in tables

**Diagnosis:**
```python
# Check schema location
schema_location = "abfss://data@<storage>.dfs.core.windows.net/schemas/autoloader/customer-api"
dbutils.fs.ls(schema_location)
```

**Resolution:**
1. **Option 1:** Allow schema evolution (recommended)
   ```python
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
   ```

2. **Option 2:** Reset schema inference
   ```bash
   # Backup and delete schema location
   dbutils.fs.rm(schema_location, recurse=True)
   # Restart pipeline to re-infer schema
   ```

3. **Option 3:** Provide explicit schema
   ```python
   .option("cloudFiles.schemaHints", "new_column STRING")
   ```

---

### 2.2 Data Quality Issues

#### Issue: High Drop Rate in Silver Layer

**Symptoms:**
- >5% of records being dropped
- Quarantine table filling up quickly

**Diagnosis:**
```sql
-- Analyze quarantine records
SELECT
    failure_reason,
    COUNT(*) AS count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
FROM dev_data_platform.silver_cleansed.silver_events_quarantine
WHERE quarantine_timestamp >= current_date()
GROUP BY failure_reason
ORDER BY count DESC;

-- Sample quarantined records
SELECT *
FROM dev_data_platform.silver_cleansed.silver_events_quarantine
WHERE quarantine_timestamp >= current_date()
LIMIT 100;
```

**Resolution:**
1. Identify root cause from failure_reason
2. If upstream data issue, contact source system team
3. If expectation too strict, adjust:
   ```python
   # Change from expect_or_drop to expect (just log)
   @dlt.expect("valid_email", "email IS NOT NULL")
   ```

4. Reprocess quarantined data after fix:
   ```python
   # Create temporary table from quarantine
   # Apply fixes
   # Merge back to Silver
   ```

---

#### Issue: Duplicate Records in Gold Layer

**Symptoms:**
- Unexpected row count increases
- Business metrics inflated

**Diagnosis:**
```sql
-- Check for duplicates
SELECT
    transaction_id,
    transaction_date,
    COUNT(*) AS duplicate_count
FROM prod_data_platform.gold_analytics.gold_fact_transactions
GROUP BY transaction_id, transaction_date
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
```

**Resolution:**
1. Add deduplication in Silver layer:
   ```python
   .dropDuplicates(["transaction_id", "transaction_timestamp"])
   ```

2. Create corrected Gold table:
   ```sql
   CREATE OR REPLACE TABLE prod_data_platform.gold_analytics.gold_fact_transactions_deduped AS
   SELECT DISTINCT *
   FROM prod_data_platform.gold_analytics.gold_fact_transactions;
   
   -- Swap tables
   ALTER TABLE prod_data_platform.gold_analytics.gold_fact_transactions RENAME TO gold_fact_transactions_old;
   ALTER TABLE prod_data_platform.gold_analytics.gold_fact_transactions_deduped RENAME TO gold_fact_transactions;
   ```

---

### 2.3 Performance Issues

#### Issue: Slow Query Performance

**Symptoms:**
- Queries taking >5 minutes
- Dashboards timing out

**Diagnosis:**
```sql
-- Check table statistics
DESCRIBE DETAIL prod_data_platform.gold_analytics.gold_customer_360;

-- Check file count (high count = slow performance)
SELECT numFiles FROM table_detail;

-- Analyze query plan
EXPLAIN EXTENDED
SELECT *
FROM prod_data_platform.gold_analytics.gold_customer_360
WHERE customer_segment = 'VIP';
```

**Resolution:**
1. **Optimize table:**
   ```sql
   OPTIMIZE prod_data_platform.gold_analytics.gold_customer_360
   ZORDER BY (customer_id, customer_segment);
   ```

2. **Vacuum old files:**
   ```sql
   VACUUM prod_data_platform.gold_analytics.gold_customer_360 RETAIN 168 HOURS;
   ```

3. **Enable Photon:**
   ```python
   # In DLT cluster config
   "spark.databricks.photon.enabled": "true"
   ```

4. **Add table properties:**
   ```sql
   ALTER TABLE prod_data_platform.gold_analytics.gold_customer_360
   SET TBLPROPERTIES (
       'delta.autoOptimize.optimizeWrite' = 'true',
       'delta.autoOptimize.autoCompact' = 'true'
   );
   ```

---

#### Issue: Streaming Lag Increasing

**Symptoms:**
- Event Hubs consumer lag growing
- Real-time data delayed by hours

**Diagnosis:**
```sql
-- Check processing rate vs input rate
SELECT
    timestamp,
    details:flow_progress.metrics.input_row_rate AS input_rate,
    details:flow_progress.metrics.processing_rate AS processing_rate,
    details:flow_progress.metrics.backlog_bytes AS backlog
FROM event_log('bronze-kafka-ingestion')
WHERE details:flow_progress IS NOT NULL
ORDER BY timestamp DESC
LIMIT 50;
```

**Resolution:**
1. **Increase cluster size:**
   ```yaml
   # In databricks.yml
   clusters:
     - label: default
       autoscale:
         min_workers: 4  # Increase from 2
         max_workers: 16  # Increase from 8
   ```

2. **Increase trigger interval:**
   ```python
   # Reduce maxEventsPerTrigger to prevent backpressure
   .option("maxEventsPerTrigger", 5000)  # Reduce from 10000
   ```

3. **Optimize transformation logic:**
   ```python
   # Avoid expensive operations in streaming
   # Move complex joins to batch Silver/Gold layers
   ```

---

## 3. Maintenance Tasks

### 3.1 Weekly Maintenance

**Every Monday at 2 AM:**

```sql
-- 1. Optimize frequently queried tables
OPTIMIZE prod_data_platform.gold_analytics.gold_customer_360
ZORDER BY (customer_id, customer_segment);

OPTIMIZE prod_data_platform.gold_analytics.gold_fact_transactions
ZORDER BY (customer_id, transaction_date);

-- 2. Vacuum to remove old files (retain 7 days)
VACUUM prod_data_platform.silver_cleansed.silver_events_clean RETAIN 168 HOURS;
VACUUM prod_data_platform.gold_analytics.gold_customer_360 RETAIN 168 HOURS;
```

**Review metrics:**
```sql
-- Weekly data quality trend
SELECT
    DATE_TRUNC('week', metric_date) AS week,
    table_name,
    AVG(avg_drop_rate) AS avg_weekly_drop_rate,
    SUM(total_rows) AS total_weekly_rows
FROM monitoring.data_quality_metrics
WHERE metric_date >= current_date() - INTERVAL 4 WEEKS
GROUP BY week, table_name
ORDER BY week DESC, avg_weekly_drop_rate DESC;
```

---

### 3.2 Monthly Maintenance

**First Sunday of each month:**

```sql
-- 1. Analyze table growth
SELECT
    table_catalog,
    table_schema,
    table_name,
    size_in_bytes / 1024 / 1024 / 1024 AS size_gb,
    num_files
FROM system.information_schema.tables
WHERE table_catalog = 'prod_data_platform'
ORDER BY size_in_bytes DESC;

-- 2. Review and clean up old data in Bronze
DELETE FROM prod_data_platform.bronze_raw.bronze_kafka_events_raw
WHERE ingestion_date < current_date() - INTERVAL 90 DAYS;

VACUUM prod_data_platform.bronze_raw.bronze_kafka_events_raw RETAIN 0 HOURS;
```

**Cost analysis:**
```sql
-- Monthly cost tracking
SELECT
    DATE_TRUNC('month', cost_date) AS month,
    SUM(estimated_cost_usd) AS monthly_cost
FROM monitoring.pipeline_costs
WHERE cost_date >= current_date() - INTERVAL 6 MONTHS
GROUP BY month
ORDER BY month DESC;
```

---

### 3.3 Quarterly Maintenance

**Every 3 months:**

1. **Review Unity Catalog permissions:**
   ```sql
   -- Audit all grants
   SELECT
       grantee,
       object_type,
       object_key,
       privilege
   FROM system.information_schema.grants
   WHERE catalog_name = 'prod_data_platform'
   ORDER BY grantee;
   
   -- Identify unused permissions
   ```

2. **Review cluster configurations:**
   - Right-size clusters based on actual usage
   - Review auto-scaling settings
   - Update node types if needed

3. **Databricks runtime upgrades:**
   - Test new DBR versions in dev
   - Plan rollout to test → prod

4. **Review data retention policies:**
   ```sql
   -- Identify large, old tables
   SELECT
       table_name,
       size_in_bytes / 1024 / 1024 / 1024 AS size_gb,
       created AS created_date
   FROM system.information_schema.tables
   WHERE table_catalog = 'prod_data_platform'
     AND created < current_date() - INTERVAL 1 YEAR
   ORDER BY size_in_bytes DESC;
   ```

---

## 4. Incident Response

### 4.1 Severity Levels

| Severity | Description | Response Time | Escalation |
|----------|-------------|---------------|------------|
| **P1 - Critical** | Production pipeline down >1 hour, complete data loss | 15 minutes | Immediate page |
| **P2 - High** | Degraded performance, partial data loss | 1 hour | Email + Slack |
| **P3 - Medium** | Non-critical issues, workarounds available | 4 hours | Slack notification |
| **P4 - Low** | Minor issues, no business impact | Next business day | Ticket only |

### 4.2 Incident Response Process

1. **Detect:** Alert fires or user reports issue
2. **Acknowledge:** On-call engineer acknowledges within SLA
3. **Investigate:** Use diagnosis steps from this runbook
4. **Mitigate:** Apply immediate fix or workaround
5. **Resolve:** Implement permanent solution
6. **Document:** Update runbook with new findings
7. **Post-mortem:** Conduct review for P1/P2 incidents

### 4.3 Communication Template

**Slack Update (every 30 minutes during incident):**
```
🚨 Incident Update - <Severity>
Issue: <Brief description>
Impact: <What's affected>
Status: <Investigating / Mitigating / Resolved>
ETA: <Expected resolution time>
Last Update: <Timestamp>
On-call: <Engineer name>
```

**Post-Incident Report:**
```
Incident Summary
- Date/Time: 
- Duration: 
- Severity: 
- Root Cause: 
- Impact: 
- Resolution: 
- Prevention: 
- Action Items:
```

---

## 5. Backup & Recovery

### 5.1 Backup Strategy

**Daily Automated Backups:**
1. Unity Catalog metastore backup (automatic)
2. Critical table snapshots
3. Pipeline configuration backup

```python
# Backup critical tables daily
from datetime import datetime

backup_date = datetime.now().strftime("%Y%m%d")

tables_to_backup = [
    "prod_data_platform.gold_analytics.gold_customer_360",
    "prod_data_platform.gold_analytics.gold_fact_transactions"
]

for table in tables_to_backup:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}_backup_{backup_date}
        SHALLOW CLONE {table}
    """)
```

### 5.2 Disaster Recovery

**RTO (Recovery Time Objective):** 4 hours  
**RPO (Recovery Point Objective):** 1 hour

**Recovery Steps:**

1. **Assess damage:**
   ```sql
   -- Check last good version
   DESCRIBE HISTORY prod_data_platform.gold_analytics.gold_customer_360;
   ```

2. **Restore from backup:**
   ```sql
   -- Option 1: Restore from time travel
   RESTORE TABLE prod_data_platform.gold_analytics.gold_customer_360
   TO VERSION AS OF 100;
   
   -- Option 2: Restore from backup
   CREATE OR REPLACE TABLE prod_data_platform.gold_analytics.gold_customer_360
   AS SELECT * FROM prod_data_platform.gold_analytics.gold_customer_360_backup_20260208;
   ```

3. **Verify data integrity:**
   ```sql
   -- Compare row counts
   SELECT COUNT(*) FROM prod_data_platform.gold_analytics.gold_customer_360;
   ```

4. **Resume pipelines:**
   ```bash
   databricks pipelines start-update --pipeline-id <pipeline-id>
   ```

---

## 6. On-Call Rotation

### 6.1 On-Call Schedule

- **Primary:** Rotates weekly, Monday 9 AM
- **Secondary:** Backup for escalations
- **Manager:** Escalation point for P1 incidents

### 6.2 On-Call Handoff Checklist

**Outgoing engineer provides:**
- [ ] Summary of past week issues
- [ ] Any ongoing investigations
- [ ] Upcoming maintenance windows
- [ ] Known issues or workarounds

**Incoming engineer verifies:**
- [ ] Access to all systems (Databricks, Azure, Slack)
- [ ] PagerDuty notifications working
- [ ] Runbook up to date
- [ ] Contact information current

---

## 7. Emergency Contacts

| Role | Contact | Primary | Secondary |
|------|---------|---------|-----------|
| **Data Platform Lead** | lead@company.com | 555-0100 | Slack DM |
| **On-Call Engineer** | oncall@company.com | PagerDuty | 555-0101 |
| **Azure Support** | azure.microsoft.com/support | Portal | 1-800-xxx-xxxx |
| **Databricks Support** | help.databricks.com | Portal | support@databricks.com |

---

## 8. Useful Queries & Commands

### 8.1 Quick Diagnostics

```sql
-- Pipeline health snapshot
SELECT
    'Bronze' AS layer,
    COUNT(DISTINCT table_name) AS table_count,
    SUM(size_in_bytes) / 1024 / 1024 / 1024 AS total_gb
FROM system.information_schema.tables
WHERE table_schema = 'bronze_raw'

UNION ALL

SELECT 'Silver', COUNT(DISTINCT table_name), SUM(size_in_bytes) / 1024 / 1024 / 1024
FROM system.information_schema.tables
WHERE table_schema = 'silver_cleansed'

UNION ALL

SELECT 'Gold', COUNT(DISTINCT table_name), SUM(size_in_bytes) / 1024 / 1024 / 1024
FROM system.information_schema.tables
WHERE table_schema = 'gold_analytics';
```

### 8.2 Performance Checks

```bash
# Check cluster status
databricks clusters list

# Check pipeline status
databricks pipelines get --pipeline-id <pipeline-id>

# View recent runs
databricks pipelines list-updates --pipeline-id <pipeline-id>
```

---

## 9. Runbook Updates

This runbook is a living document. Update it:
- After resolving novel incidents
- When adding new pipelines or tables
- After infrastructure changes
- During quarterly reviews

**Last Updated:** February 8, 2026  
**Next Review:** May 8, 2026

---

**References:**
- ← [09-Deployment-CICD.md](09-Deployment-CICD.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
- ↑ [00-Index.md](00-Index.md)
- [Databricks Operations Guide](https://learn.microsoft.com/azure/databricks/admin/)
