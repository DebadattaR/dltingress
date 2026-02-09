# 03 - Bronze Layer Implementation

**Related Documents:**
- ← [02-Infrastructure-Setup.md](02-Infrastructure-Setup.md) - Infrastructure Setup
- → [04-Silver-Layer.md](04-Silver-Layer.md) - Silver Layer Implementation
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview

---

## 1. Bronze Layer Overview

### 1.1 Purpose
The Bronze layer captures raw data **exactly as received** from source systems with minimal transformation. This layer serves as:
- **Immutable audit trail** of source data
- **Foundation for reprocessing** if transformations need to change
- **Schema evolution tracking** for upstream changes

### 1.2 Design Principles
- ✅ Accept all data (no filtering)
- ✅ Preserve original structure
- ✅ Add metadata for traceability
- ✅ Append-only (no updates/deletes)
- ❌ No business logic
- ❌ No data quality filtering

---

## 2. Unity Catalog Setup

### 2.1 Create Bronze Schema

```sql
-- Run this in Databricks SQL or notebook

USE CATALOG dev_data_platform;

CREATE SCHEMA IF NOT EXISTS bronze_raw
COMMENT 'Raw data ingestion layer - no transformations'
LOCATION 'abfss://data@<storage_account>.dfs.core.windows.net/bronze/';

-- Verify
DESCRIBE SCHEMA EXTENDED bronze_raw;
```

### 2.2 Set Default Permissions

```sql
-- Grant appropriate permissions (run as admin)

GRANT USE SCHEMA ON SCHEMA dev_data_platform.bronze_raw TO `data-engineers`;
GRANT CREATE TABLE ON SCHEMA dev_data_platform.bronze_raw TO `data-engineers`;
GRANT SELECT ON SCHEMA dev_data_platform.bronze_raw TO `data-engineers`;
GRANT MODIFY ON SCHEMA dev_data_platform.bronze_raw TO `data-engineers`;
```

---

## 3. Kafka/Event Hubs Ingestion (90%)

### 3.1 Event Hubs Configuration

**Setup in Azure Portal:**
1. Create Event Hubs namespace
2. Create event hub (topic): `customer-events`, `transaction-events`
3. Create consumer group: `databricks-lakeflow`
4. Get connection string

**Store in Key Vault:**
```bash
# Azure CLI
az keyvault secret set \
  --vault-name <your-key-vault> \
  --name eventhub-connection-string \
  --value "Endpoint=sb://<namespace>.servicebus.windows.net/;..."
```

### 3.2 Lakeflow Bronze Pipeline - Kafka/Event Hubs

**File:** `pipelines/bronze_kafka_ingestion.py`

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
# CONFIGURATION
# =============================================================================

# Event Hubs connection (retrieve from Databricks secrets)
eventhub_connection_string = dbutils.secrets.get(
    scope="key-vault-scope", 
    key="eventhub-connection-string"
)

# Event Hub configuration
eventhub_config = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        eventhub_connection_string
    ),
    "eventhubs.consumerGroup": "databricks-lakeflow",
    "maxEventsPerTrigger": 10000,  # Backpressure control
    "startingPosition": "-1"  # Start from latest
}

# =============================================================================
# BRONZE TABLE: KAFKA EVENTS RAW
# =============================================================================

@dlt.table(
    name="bronze_kafka_events_raw",
    comment="Raw events from Azure Event Hubs - no transformations",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "enqueuedTime",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["ingestion_date"]
)
def bronze_kafka_events_raw():
    """
    Ingest raw Kafka/Event Hubs messages.
    
    Schema includes:
    - body: Raw message content (binary)
    - partition: Kafka partition
    - offset: Message offset
    - enqueuedTime: Event Hubs timestamp
    - Additional metadata fields
    """
    return (
        spark.readStream
            .format("eventhubs")
            .options(**eventhub_config)
            .load()
            # Add ingestion metadata
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("event-hubs"))
            .withColumn("pipeline_run_id", lit(spark.conf.get("spark.databricks.clusterUsageTags.pipelineId", "local")))
    )

# =============================================================================
# BRONZE TABLE: PARSED KAFKA EVENTS (OPTIONAL)
# =============================================================================

@dlt.table(
    name="bronze_kafka_events_parsed",
    comment="Kafka events with parsed JSON body - still bronze quality",
    table_properties={
        "quality": "bronze"
    },
    partition_cols=["ingestion_date"]
)
def bronze_kafka_events_parsed():
    """
    Parse JSON from Kafka message body.
    Still considered Bronze - only converting binary to string/JSON.
    """
    
    # Define expected schema for JSON payload
    # This is flexible - use schema hints or infer
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("payload", MapType(StringType(), StringType()), True)
    ])
    
    return (
        dlt.read_stream("bronze_kafka_events_raw")
            # Convert binary body to string
            .withColumn("body_string", col("body").cast("string"))
            # Parse JSON
            .withColumn("event_data", from_json(col("body_string"), event_schema))
            # Flatten for easier querying
            .select(
                col("event_data.*"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("enqueuedTime").alias("event_hub_time"),
                col("ingestion_timestamp"),
                col("ingestion_date"),
                col("source_system")
            )
    )

# =============================================================================
# EXAMPLE: MULTIPLE EVENT HUB TOPICS
# =============================================================================

@dlt.table(
    name="bronze_transaction_events_raw",
    comment="Raw transaction events from dedicated Event Hub",
    partition_cols=["ingestion_date"]
)
def bronze_transaction_events_raw():
    """
    Separate table for transaction events from different Event Hub.
    """
    
    transaction_eventhub_config = {
        "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            dbutils.secrets.get(scope="key-vault-scope", key="transaction-eventhub-connection")
        ),
        "eventhubs.consumerGroup": "databricks-lakeflow",
        "maxEventsPerTrigger": 5000
    }
    
    return (
        spark.readStream
            .format("eventhubs")
            .options(**transaction_eventhub_config)
            .load()
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("transaction-event-hub"))
    )
```

### 3.3 Running the Kafka Pipeline

**Create DLT Pipeline in Databricks:**
1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   - **Name:** `bronze-kafka-ingestion`
   - **Product Edition:** Advanced (for Unity Catalog)
   - **Notebook Libraries:** `pipelines/bronze_kafka_ingestion.py`
   - **Storage Location:** Let DLT manage
   - **Target:** `dev_data_platform.bronze_raw`
   - **Pipeline Mode:** Continuous (for streaming)
   - **Channel:** Current
   - **Cluster:** Autoscaling 2-8 workers, Standard_E8s_v3

4. Click **Create** and then **Start**

---

## 4. REST API Ingestion (~5%)

### 4.1 Landing Zone Pattern

API data follows a two-step process:
1. **Azure Data Factory** calls REST API → writes JSON to ADLS landing zone
2. **Lakeflow AutoLoader** monitors landing zone → ingests to Bronze table

**Landing Zone Structure:**
```
abfss://data@<storage_account>.dfs.core.windows.net/
└── landing-zones/
    └── api-data/
        ├── customer-api/
        │   ├── 2026-02-08-10-00-00.json
        │   └── 2026-02-08-11-00-00.json
        └── product-api/
            └── 2026-02-08-10-00-00.json
```

### 4.2 Azure Data Factory Pipeline

**Simplified ADF Pipeline Steps:**
1. **Web Activity:** Call REST API with authentication
2. **Copy Data:** Write JSON response to ADLS Gen2
3. **Schedule:** Hourly trigger

*(Full ADF configuration in [02-Infrastructure-Setup.md](02-Infrastructure-Setup.md))*

### 4.3 Lakeflow Bronze Pipeline - REST API

**File:** `pipelines/bronze_api_ingestion.py`

```python
import dlt
from pyspark.sql.functions import *

# =============================================================================
# CONFIGURATION
# =============================================================================

# Landing zone paths
API_LANDING_BASE = "abfss://data@<storage_account>.dfs.core.windows.net/landing-zones/api-data"
CUSTOMER_API_LANDING = f"{API_LANDING_BASE}/customer-api"
PRODUCT_API_LANDING = f"{API_LANDING_BASE}/product-api"

# Schema location for AutoLoader checkpointing
SCHEMA_LOCATION_BASE = "abfss://data@<storage_account>.dfs.core.windows.net/schemas/autoloader"

# =============================================================================
# BRONZE TABLE: CUSTOMER API RAW
# =============================================================================

@dlt.table(
    name="bronze_customer_api_raw",
    comment="Raw customer data from external API",
    table_properties={
        "quality": "bronze"
    },
    partition_cols=["ingestion_date"]
)
def bronze_customer_api_raw():
    """
    Ingest JSON files from customer API landing zone using AutoLoader.
    
    AutoLoader features:
    - Schema inference and evolution
    - Exactly-once processing
    - Automatic file tracking
    - Efficient rescanning
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/customer-api")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.maxFilesPerTrigger", 100)
            .load(CUSTOMER_API_LANDING)
            # Add metadata
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("customer-api"))
            .withColumn("source_file", input_file_name())
    )

# =============================================================================
# BRONZE TABLE: PRODUCT API RAW
# =============================================================================

@dlt.table(
    name="bronze_product_api_raw",
    comment="Raw product data from external API",
    table_properties={
        "quality": "bronze"
    },
    partition_cols=["ingestion_date"]
)
def bronze_product_api_raw():
    """
    Ingest JSON files from product API landing zone.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/product-api")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(PRODUCT_API_LANDING)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("product-api"))
            .withColumn("source_file", input_file_name())
    )

# =============================================================================
# EXAMPLE: API WITH NESTED JSON
# =============================================================================

@dlt.table(
    name="bronze_enrichment_api_raw",
    comment="Raw enrichment data with complex nested structure",
    partition_cols=["ingestion_date"]
)
def bronze_enrichment_api_raw():
    """
    Some APIs return deeply nested JSON.
    Keep it as-is in Bronze, flatten in Silver.
    """
    
    # Optional: Provide schema hints for critical fields
    schema_hints = "id STRING, created_at TIMESTAMP"
    
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/enrichment-api")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{API_LANDING_BASE}/enrichment-api")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("enrichment-api"))
    )
```

### 4.4 Running the API Pipeline

**Create DLT Pipeline:**
1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure:
   - **Name:** `bronze-api-ingestion`
   - **Notebook Libraries:** `pipelines/bronze_api_ingestion.py`
   - **Target:** `dev_data_platform.bronze_raw`
   - **Pipeline Mode:** Triggered (runs on schedule or manual)
   - **Cluster:** Autoscaling 2-4 workers

4. **Schedule:** Run every hour to pick up new API files

---

## 5. SFTP File Ingestion (~5%)

### 5.1 Landing Zone Pattern

Similar to API ingestion:
1. **Azure Data Factory** downloads files from SFTP → ADLS landing zone
2. **Lakeflow AutoLoader** monitors landing zone → ingests to Bronze

**Landing Zone Structure:**
```
abfss://data@<storage_account>.dfs.core.windows.net/
└── landing-zones/
    └── sftp-data/
        ├── partner-reconciliation/
        │   ├── recon_2026-02-08.csv
        │   └── recon_2026-02-07.csv
        └── legacy-exports/
            └── export_20260208.parquet
```

### 5.2 Lakeflow Bronze Pipeline - SFTP

**File:** `pipelines/bronze_sftp_ingestion.py`

```python
import dlt
from pyspark.sql.functions import *

# =============================================================================
# CONFIGURATION
# =============================================================================

SFTP_LANDING_BASE = "abfss://data@<storage_account>.dfs.core.windows.net/landing-zones/sftp-data"
RECONCILIATION_LANDING = f"{SFTP_LANDING_BASE}/partner-reconciliation"
LEGACY_LANDING = f"{SFTP_LANDING_BASE}/legacy-exports"

SCHEMA_LOCATION_BASE = "abfss://data@<storage_account>.dfs.core.windows.net/schemas/autoloader"

# =============================================================================
# BRONZE TABLE: PARTNER RECONCILIATION FILES
# =============================================================================

@dlt.table(
    name="bronze_partner_recon_raw",
    comment="Raw reconciliation files from partner SFTP",
    table_properties={
        "quality": "bronze"
    },
    partition_cols=["ingestion_date"]
)
def bronze_partner_recon_raw():
    """
    Ingest CSV files from partner reconciliation SFTP.
    
    Files follow naming convention: recon_YYYY-MM-DD.csv
    """
    
    # Schema hints for CSV (optional but recommended)
    schema_hints = """
        transaction_id STRING,
        transaction_date DATE,
        amount DECIMAL(18,2),
        currency STRING,
        status STRING
    """
    
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/partner-recon")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("cloudFiles.inferColumnTypes", "true")
            # CSV-specific options
            .option("header", "true")
            .option("inferSchema", "false")  # Use schema hints instead
            .option("delimiter", ",")
            .option("quote", "\"")
            .option("escape", "\\")
            .load(RECONCILIATION_LANDING)
            # Add metadata
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("partner-sftp"))
            .withColumn("source_file", input_file_name())
            # Extract file date from filename
            .withColumn(
                "file_date", 
                regexp_extract(input_file_name(), r"recon_(\d{4}-\d{2}-\d{2})", 1)
            )
    )

# =============================================================================
# BRONZE TABLE: LEGACY SYSTEM EXPORTS
# =============================================================================

@dlt.table(
    name="bronze_legacy_exports_raw",
    comment="Raw exports from legacy system via SFTP",
    table_properties={
        "quality": "bronze"
    },
    partition_cols=["ingestion_date"]
)
def bronze_legacy_exports_raw():
    """
    Ingest Parquet files from legacy system.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/legacy-exports")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(LEGACY_LANDING)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("legacy-mainframe"))
            .withColumn("source_file", input_file_name())
    )

# =============================================================================
# EXAMPLE: MULTI-FORMAT HANDLING
# =============================================================================

@dlt.table(
    name="bronze_mixed_format_raw",
    comment="Handle both JSON and CSV from same SFTP location"
)
def bronze_mixed_format_raw():
    """
    AutoLoader can infer format, but explicit is better for production.
    If you have mixed formats, create separate tables per format.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")  # or "csv"
            .option("cloudFiles.schemaLocation", f"{SCHEMA_LOCATION_BASE}/mixed-format")
            .load(f"{SFTP_LANDING_BASE}/mixed-format")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_system", lit("sftp-mixed"))
    )
```

### 5.3 Running the SFTP Pipeline

**Create DLT Pipeline:**
1. **Name:** `bronze-sftp-ingestion`
2. **Notebook Libraries:** `pipelines/bronze_sftp_ingestion.py`
3. **Target:** `dev_data_platform.bronze_raw`
4. **Pipeline Mode:** Triggered
5. **Schedule:** Daily at 6 AM (or after SFTP files arrive)

---

## 6. Unified Bronze Pipeline (Optional)

You can combine all Bronze ingestion sources into a single pipeline:

**File:** `pipelines/bronze_unified.py`

```python
import dlt
from pyspark.sql.functions import *

# Import all Bronze table definitions
# Kafka/Event Hubs tables
exec(open("bronze_kafka_ingestion.py").read())

# API tables
exec(open("bronze_api_ingestion.py").read())

# SFTP tables
exec(open("bronze_sftp_ingestion.py").read())

# This single pipeline will manage all Bronze ingestion
# DLT will automatically handle dependencies and orchestration
```

---

## 7. Monitoring & Validation

### 7.1 Check Pipeline Status

```sql
-- Query event logs
SELECT 
    timestamp,
    message,
    level,
    details:flow_name as flow_name
FROM event_log('bronze-kafka-ingestion')
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC
LIMIT 100;
```

### 7.2 Validate Data Ingestion

```sql
-- Check row counts
SELECT 
    'bronze_kafka_events_raw' as table_name,
    COUNT(*) as row_count,
    MAX(ingestion_timestamp) as latest_ingestion
FROM dev_data_platform.bronze_raw.bronze_kafka_events_raw
UNION ALL
SELECT 
    'bronze_customer_api_raw',
    COUNT(*),
    MAX(ingestion_timestamp)
FROM dev_data_platform.bronze_raw.bronze_customer_api_raw;

-- Check for schema evolution
DESCRIBE HISTORY dev_data_platform.bronze_raw.bronze_kafka_events_raw;
```

### 7.3 Monitor Streaming Lag

```python
# In notebook
display(
    spark.sql("""
        SELECT 
            timestamp,
            details:flow_name as table_name,
            details:metrics.num_output_rows as rows_processed,
            details:metrics.input_row_rate as input_rate
        FROM event_log('bronze-kafka-ingestion')
        WHERE details:metrics IS NOT NULL
        ORDER BY timestamp DESC
    """)
)
```

---

## 8. Troubleshooting

### 8.1 Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Event Hubs connection failed | Pipeline error on start | Verify connection string in Key Vault |
| Schema inference slow | AutoLoader takes long time | Provide explicit schema hints |
| Backpressure/lag | Streaming lag increasing | Increase `maxEventsPerTrigger` or cluster size |
| Out of memory | Executor failures | Use memory-optimized nodes, reduce trigger size |
| File already processed | Duplicate data | Check AutoLoader checkpoint location |

### 8.2 Reset Streaming Checkpoint

```python
# If you need to reprocess data, reset checkpoint
# WARNING: Use with caution in production

dbutils.fs.rm(
    "abfss://data@<storage>.dfs.core.windows.net/schemas/autoloader/customer-api",
    recurse=True
)
```

---

## 9. Best Practices

### 9.1 Naming Conventions

- **Table names:** `bronze_<source>_<entity>_raw`
- **File names:** `bronze_<source>_ingestion.py`
- **Schemas:** `bronze_raw` (consistent across environments)

### 9.2 Performance Tuning

```python
# Optimize for high-volume streaming
@dlt.table(
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
```

### 9.3 Cost Optimization

- Use **Triggered mode** for batch sources (API, SFTP)
- Use **Continuous mode** only for truly real-time needs
- Enable **auto-termination** on DLT clusters
- Consider **Photon** for better price/performance

---

## 10. Next Steps

Bronze layer complete! Now move to data cleansing:

→ **[04-Silver-Layer.md](04-Silver-Layer.md)** - Implement data quality and transformations

---

**References:**
- ← [02-Infrastructure-Setup.md](02-Infrastructure-Setup.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
- → [04-Silver-Layer.md](04-Silver-Layer.md)
