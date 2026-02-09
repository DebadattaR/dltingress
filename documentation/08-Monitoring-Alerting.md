# 08 - Monitoring & Alerting

**Related Documents:**
- ← [07-Security-Governance.md](07-Security-Governance.md) - Security & Governance
- → [09-Deployment-CICD.md](09-Deployment-CICD.md) - Deployment & CI/CD
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview

---

## 1. Monitoring Strategy

### 1.1 Four Pillars of Observability

1. **Pipeline Health** - Is the pipeline running successfully?
2. **Data Quality** - Is the data meeting expectations?
3. **Performance** - Is processing fast enough?
4. **Cost** - Are we staying within budget?

### 1.2 Monitoring Layers

```
Application Layer (Lakeflow Pipelines)
    ↓
Platform Layer (Databricks, Event Hubs)
    ↓
Infrastructure Layer (Azure Resources)
    ↓
Cost Layer (Azure Cost Management)
```

---

## 2. Lakeflow Pipeline Monitoring

### 2.1 Built-in DLT Observability

**Access Pipeline Metrics:**
1. Go to **Workflows** → **Delta Live Tables**
2. Select pipeline → **Monitoring** tab

**Key Metrics:**
- Pipeline run status (success/failure)
- Processing time per update
- Row counts per table
- Data quality expectation violations
- Lineage graph

### 2.2 Query Event Logs

```sql
-- Pipeline execution history
SELECT
    timestamp,
    event_type,
    message,
    details
FROM event_log('<pipeline-id>')
WHERE event_type IN ('user_action', 'flow_definition', 'flow_progress', 'flow_failed')
ORDER BY timestamp DESC
LIMIT 100;
```

### 2.3 Monitor Streaming Lag

```sql
-- Check streaming lag for Kafka pipelines
SELECT
    timestamp,
    details:flow_definition.output_dataset AS table_name,
    details:flow_progress.metrics.backlog_bytes AS backlog_bytes,
    details:flow_progress.metrics.num_output_rows AS rows_processed,
    details:flow_progress.metrics.input_row_rate AS input_rate
FROM event_log('bronze-kafka-ingestion')
WHERE details:flow_progress IS NOT NULL
ORDER BY timestamp DESC
LIMIT 50;
```

---

## 3. Data Quality Monitoring

### 3.1 Expectations Dashboard

```sql
-- Create view for quality metrics
CREATE OR REPLACE VIEW monitoring.data_quality_metrics AS
SELECT
    date(timestamp) AS metric_date,
    details:flow_definition.output_dataset AS table_name,
    details:flow_progress.metrics.num_output_rows AS total_rows,
    size(filter(details:flow_progress.data_quality.expectations, x -> x.passed = false)) AS failed_expectations,
    details:flow_progress.data_quality.dropped_records AS dropped_records,
    ROUND(100.0 * dropped_records / NULLIF(total_rows, 0), 2) AS drop_rate_pct
FROM event_log('<pipeline-id>')
WHERE details:flow_progress.data_quality IS NOT NULL
  AND date(timestamp) >= current_date() - INTERVAL 7 DAYS;

-- Query quality trends
SELECT
    metric_date,
    table_name,
    SUM(dropped_records) AS total_dropped,
    AVG(drop_rate_pct) AS avg_drop_rate
FROM monitoring.data_quality_metrics
GROUP BY metric_date, table_name
ORDER BY metric_date DESC, avg_drop_rate DESC;
```

### 3.2 Row Count Validation

```python
# Automated row count checks in notebook
from pyspark.sql.functions import count, current_date

def validate_row_counts(catalog, schema):
    """
    Compare row counts between layers to detect anomalies.
    """
    
    tables = spark.sql(f"""
        SELECT table_name
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
    """).collect()
    
    results = []
    
    for table in tables:
        table_name = table.table_name
        row_count = spark.table(f"{catalog}.{schema}.{table_name}").count()
        results.append({
            "table": f"{catalog}.{schema}.{table_name}",
            "row_count": row_count,
            "check_timestamp": datetime.now()
        })
    
    return spark.createDataFrame(results)

# Run validation
validation_df = validate_row_counts("prod_data_platform", "silver_cleansed")
display(validation_df)
```

### 3.3 Data Freshness Monitoring

```sql
-- Monitor data freshness (time since last update)
SELECT
    table_catalog,
    table_schema,
    table_name,
    MAX(silver_processed_timestamp) AS last_update,
    TIMESTAMPDIFF(HOUR, MAX(silver_processed_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
FROM prod_data_platform.silver_cleansed.silver_events_clean
GROUP BY table_catalog, table_schema, table_name
HAVING hours_since_update > 2  -- Alert if > 2 hours old
ORDER BY hours_since_update DESC;
```

---

## 4. Azure Monitor Integration

### 4.1 Send Databricks Logs to Log Analytics

**Configure Diagnostic Settings:**
```bash
# Azure CLI
az monitor diagnostic-settings create \
  --resource /subscriptions/<sub-id>/resourceGroups/rg-data-platform-prod/providers/Microsoft.Databricks/workspaces/dbw-data-platform-prod \
  --name databricks-diagnostics \
  --workspace /subscriptions/<sub-id>/resourceGroups/rg-data-platform-prod/providers/Microsoft.OperationalInsights/workspaces/log-analytics-workspace \
  --logs '[
    {
      "category": "dbfs",
      "enabled": true
    },
    {
      "category": "clusters",
      "enabled": true
    },
    {
      "category": "jobs",
      "enabled": true
    }
  ]'
```

### 4.2 Query Logs in Log Analytics

```kql
// Kusto Query Language (KQL)

// Failed pipeline runs
DatabricksDeltaPipelines
| where TimeGenerated > ago(24h)
| where Status == "FAILED"
| project TimeGenerated, PipelineName, ErrorMessage
| order by TimeGenerated desc

// Cluster utilization
DatabricksClusters
| where TimeGenerated > ago(1h)
| summarize avg(CpuPercent), avg(MemoryPercent) by ClusterId, bin(TimeGenerated, 5m)
| render timechart
```

---

## 5. Alerting Strategy

### 5.1 Azure Monitor Alerts

**Pipeline Failure Alert:**
```bash
# Create action group
az monitor action-group create \
  --name ag-pipeline-failures \
  --resource-group rg-data-platform-prod \
  --short-name PipelineAlerts \
  --email-receiver name=DataEngTeam address=data-eng@company.com

# Create alert rule
az monitor metrics alert create \
  --name alert-pipeline-failure \
  --resource-group rg-data-platform-prod \
  --scopes /subscriptions/<sub-id>/resourceGroups/rg-data-platform-prod/providers/Microsoft.Databricks/workspaces/dbw-data-platform-prod \
  --condition "count > 0" \
  --description "Alert when pipeline fails" \
  --evaluation-frequency 5m \
  --window-size 15m \
  --action ag-pipeline-failures
```

### 5.2 Custom Alerts via Databricks

```python
# File: monitoring/alert_on_quality.py

import requests
from datetime import datetime

def check_data_quality_and_alert():
    """
    Check data quality metrics and send alerts if thresholds exceeded.
    """
    
    # Query quality metrics
    quality_df = spark.sql("""
        SELECT
            table_name,
            drop_rate_pct,
            total_rows,
            dropped_records
        FROM monitoring.data_quality_metrics
        WHERE metric_date = current_date()
          AND drop_rate_pct > 5  -- Alert threshold: 5% drop rate
    """)
    
    if quality_df.count() > 0:
        # Send alert to Slack/Teams
        webhook_url = dbutils.secrets.get("key-vault-scope", "slack-webhook-url")
        
        for row in quality_df.collect():
            message = f"""
            🚨 Data Quality Alert
            Table: {row.table_name}
            Drop Rate: {row.drop_rate_pct}%
            Dropped Records: {row.dropped_records} / {row.total_rows}
            Time: {datetime.now()}
            """
            
            requests.post(webhook_url, json={"text": message})

# Schedule this notebook to run hourly
check_data_quality_and_alert()
```

### 5.3 Alert Severity Levels

| Severity | Condition | Action | SLA |
|----------|-----------|--------|-----|
| **Critical** | Pipeline failure, >10% drop rate | Page on-call engineer | 15 minutes |
| **High** | Data freshness >4 hours, 5-10% drop rate | Email + Slack | 1 hour |
| **Medium** | Streaming lag >30 min, 2-5% drop rate | Slack notification | 4 hours |
| **Low** | Performance degradation, <2% drop rate | Log for review | Next business day |

---

## 6. Performance Monitoring

### 6.1 Pipeline Execution Time

```sql
-- Track pipeline execution duration
SELECT
    date(timestamp) AS run_date,
    details:pipeline_name AS pipeline_name,
    MIN(timestamp) AS start_time,
    MAX(timestamp) AS end_time,
    TIMESTAMPDIFF(MINUTE, MIN(timestamp), MAX(timestamp)) AS duration_minutes
FROM event_log('<pipeline-id>')
WHERE event_type IN ('create_update', 'update_complete')
GROUP BY date(timestamp), details:pipeline_name
ORDER BY run_date DESC;
```

### 6.2 Query Performance

```sql
-- Enable query history
SET spark.databricks.queryWatchdog.enabled = true;

-- Analyze slow queries
SELECT
    query_id,
    query_text,
    execution_time_ms,
    read_bytes,
    result_rows
FROM system.query.history
WHERE execution_time_ms > 60000  -- Queries slower than 1 minute
  AND start_time >= current_date() - INTERVAL 7 DAYS
ORDER BY execution_time_ms DESC
LIMIT 20;
```

### 6.3 Cluster Metrics

```python
# Get cluster metrics via Databricks API
import requests

databricks_instance = "https://adb-<workspace-id>.azuredatabricks.net"
token = dbutils.secrets.get("key-vault-scope", "databricks-token")

headers = {"Authorization": f"Bearer {token}"}

# Get cluster metrics
response = requests.get(
    f"{databricks_instance}/api/2.0/clusters/get",
    headers=headers,
    params={"cluster_id": "<cluster-id>"}
)

cluster_info = response.json()
print(f"State: {cluster_info['state']}")
print(f"Workers: {cluster_info['num_workers']}")
```

---

## 7. Cost Monitoring

### 7.1 Azure Cost Management

**Query costs by resource:**
```bash
# Azure CLI
az consumption usage list \
  --start-date 2026-02-01 \
  --end-date 2026-02-28 \
  --query "[?contains(instanceId, 'databricks')].{Resource:instanceId, Cost:pretaxCost}" \
  --output table
```

### 7.2 Databricks Cost Tracking

```sql
-- Create cost monitoring view
CREATE OR REPLACE VIEW monitoring.pipeline_costs AS
SELECT
    date(timestamp) AS cost_date,
    details:cluster_id AS cluster_id,
    details:cluster_size AS cluster_size,
    SUM(details:execution_duration_ms) / 1000 / 3600 AS total_hours,
    SUM(details:execution_duration_ms) / 1000 / 3600 * <dbu_rate> AS estimated_cost_usd
FROM event_log('<pipeline-id>')
WHERE details:cluster_id IS NOT NULL
GROUP BY cost_date, cluster_id, cluster_size
ORDER BY cost_date DESC;

-- Analyze cost trends
SELECT
    cost_date,
    SUM(estimated_cost_usd) AS daily_cost
FROM monitoring.pipeline_costs
GROUP BY cost_date
ORDER BY cost_date DESC;
```

### 7.3 Cost Optimization Alerts

```python
# Alert if daily cost exceeds threshold
def check_cost_threshold():
    cost_df = spark.sql("""
        SELECT SUM(estimated_cost_usd) AS daily_cost
        FROM monitoring.pipeline_costs
        WHERE cost_date = current_date()
    """)
    
    daily_cost = cost_df.collect()[0].daily_cost
    
    if daily_cost > 500:  # Alert threshold: $500/day
        send_alert(f"Daily cost exceeded threshold: ${daily_cost}")
```

---

## 8. Dashboards

### 8.1 Databricks SQL Dashboard

**Create SQL Dashboard:**
1. Go to **SQL** → **Dashboards** → **Create Dashboard**
2. Add visualizations:

**Pipeline Health Widget:**
```sql
SELECT
    details:pipeline_name AS pipeline_name,
    COUNT(CASE WHEN event_type = 'update_complete' THEN 1 END) AS successful_runs,
    COUNT(CASE WHEN event_type = 'flow_failed' THEN 1 END) AS failed_runs
FROM event_log('<pipeline-id>')
WHERE date(timestamp) >= current_date() - INTERVAL 7 DAYS
GROUP BY details:pipeline_name;
```

**Data Quality Widget:**
```sql
SELECT
    metric_date,
    table_name,
    avg_drop_rate
FROM monitoring.data_quality_metrics
WHERE metric_date >= current_date() - INTERVAL 30 DAYS
ORDER BY metric_date DESC;
```

### 8.2 Power BI Integration

```python
# Export metrics for Power BI
monitoring_df = spark.sql("""
    SELECT
        date(timestamp) AS metric_date,
        details:flow_definition.output_dataset AS table_name,
        details:flow_progress.metrics.num_output_rows AS row_count,
        details:flow_progress.data_quality.dropped_records AS dropped_records
    FROM event_log('<pipeline-id>')
    WHERE date(timestamp) >= current_date() - INTERVAL 90 DAYS
""")

# Write to Gold layer for Power BI access
monitoring_df.write.format("delta").mode("overwrite").saveAsTable(
    "prod_data_platform.gold_analytics.monitoring_metrics"
)
```

---

## 9. Incident Response Runbook

### 9.1 Pipeline Failure

**Diagnosis:**
1. Check event logs for error messages
2. Verify source system availability
3. Check data quality expectation failures

**Resolution:**
```sql
-- View error details
SELECT
    timestamp,
    message,
    details:error
FROM event_log('<pipeline-id>')
WHERE event_type = 'flow_failed'
ORDER BY timestamp DESC
LIMIT 10;

-- Restart pipeline
-- Via UI: Workflows → Delta Live Tables → Select Pipeline → Start
```

### 9.2 Data Quality Issues

**Diagnosis:**
```sql
-- Check quarantine table
SELECT *
FROM dev_data_platform.silver_cleansed.silver_events_quarantine
WHERE quarantine_timestamp >= current_timestamp() - INTERVAL 1 HOUR
LIMIT 100;
```

**Resolution:**
- Review quarantined records
- Fix upstream data issues
- Adjust expectations if needed
- Reprocess data

### 9.3 Performance Degradation

**Diagnosis:**
```sql
-- Check for data skew
SELECT
    customer_id,
    COUNT(*) AS row_count
FROM prod_data_platform.silver_cleansed.silver_events_clean
WHERE event_date = current_date()
GROUP BY customer_id
ORDER BY row_count DESC
LIMIT 20;
```

**Resolution:**
- Increase cluster size
- Optimize partitioning
- Add Z-ordering
- Enable Photon engine

---

## 10. Best Practices

✅ **Set up baseline alerts** - Pipeline failures, data freshness  
✅ **Monitor data quality daily** - Track expectation violations  
✅ **Review dashboards weekly** - Identify trends early  
✅ **Cost optimization monthly** - Right-size clusters  
✅ **Test alerts regularly** - Ensure notifications work  
✅ **Document runbooks** - Faster incident resolution  
✅ **Use tiered alerting** - Avoid alert fatigue  

---

## Next Steps

Monitoring configured! Now automate deployments:

→ **[09-Deployment-CICD.md](09-Deployment-CICD.md)** - Set up CI/CD pipelines

---

**References:**
- ← [07-Security-Governance.md](07-Security-Governance.md)
- → [09-Deployment-CICD.md](09-Deployment-CICD.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
- [Azure Monitor Documentation](https://learn.microsoft.com/azure/azure-monitor/)
