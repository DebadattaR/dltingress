# 04 - Silver Layer Implementation

**Related Documents:**
- ← [03-Bronze-Layer.md](03-Bronze-Layer.md) - Bronze Layer Implementation
- → [05-Claims-Fragment-Assembly.md](05-Claims-Fragment-Assembly.md) - Claims Fragment Assembly Pattern
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview

---

## 1. Silver Layer Overview

### 1.1 Purpose
The Silver layer transforms raw Bronze data into clean, validated, business-ready datasets by:
- **Enforcing data quality** through expectations
- **Deduplicating** records
- **Standardizing** data types and formats
- **Enriching** with reference data
- **Implementing SCD Type 2** for dimensional data

### 1.2 Design Principles
- ✅ Drop or quarantine invalid records
- ✅ Enforce business rules
- ✅ Maintain data lineage
- ✅ Support schema evolution
- ✅ Enable incremental processing

---

## 2. Create Silver Schema

```sql
-- Run in Databricks SQL or notebook

USE CATALOG dev_data_platform;

CREATE SCHEMA IF NOT EXISTS silver_cleansed
COMMENT 'Cleansed and validated data layer'
LOCATION 'abfss://data@<storage_account>.dfs.core.windows.net/dev/silver/';
```

---

## 3. Silver Pipeline - Kafka Events

### 3.1 Event Cleansing and Validation

**File:** `pipelines/silver_kafka_cleansing.py`

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# =============================================================================
# SILVER TABLE: CLEANSED EVENTS
# =============================================================================

@dlt.table(
    name="silver_events_clean",
    comment="Cleansed and validated events from Kafka",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "event_id,customer_id"
    },
    partition_cols=["event_date"]
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL AND event_id != ''")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_amount", "amount >= 0 OR amount IS NULL")
@dlt.expect("recent_event", "event_timestamp >= current_date() - INTERVAL 30 DAYS")
def silver_events_clean():
    """
    Cleanse and validate Kafka events.
    
    Transformations:
    - Parse JSON from Bronze
    - Type casting
    - Deduplication
    - Data quality checks
    - Standardization
    """
    
    # Read from Bronze parsed table
    bronze_df = dlt.read_stream("bronze_kafka_events_parsed")
    
    return (
        bronze_df
            # Type casting and standardization
            .withColumn("event_id", trim(col("event_id")))
            .withColumn("event_type", upper(trim(col("event_type"))))
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("amount", col("amount").cast("decimal(18,2)"))
            
            # Extract date for partitioning
            .withColumn("event_date", to_date(col("event_timestamp")))
            
            # Add processing metadata
            .withColumn("silver_processed_timestamp", current_timestamp())
            
            # Deduplication using event_id and event_timestamp
            .dropDuplicates(["event_id", "event_timestamp"])
            
            # Select final columns
            .select(
                "event_id",
                "event_type",
                "event_timestamp",
                "event_date",
                "customer_id",
                "amount",
                "payload",
                "silver_processed_timestamp",
                "ingestion_timestamp"
            )
    )

# =============================================================================
# SILVER TABLE: TRANSACTION EVENTS WITH ENRICHMENT
# =============================================================================

@dlt.table(
    name="silver_transactions_enriched",
    comment="Transaction events enriched with customer data",
    table_properties={
        "quality": "silver"
    },
    partition_cols=["transaction_date"]
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "transaction_amount > 0")
@dlt.expect_or_fail("no_duplicate_transactions", "COUNT(*) OVER (PARTITION BY transaction_id) = 1")
def silver_transactions_enriched():
    """
    Cleanse transaction events and enrich with customer reference data.
    """
    
    # Read from Bronze
    transactions = dlt.read_stream("bronze_transaction_events_raw")
    
    # Parse JSON payload
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_timestamp", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("transaction_amount", DecimalType(18, 2), False),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    parsed = (
        transactions
            .withColumn("body_string", col("body").cast("string"))
            .withColumn("transaction_data", from_json(col("body_string"), transaction_schema))
            .select("transaction_data.*", "ingestion_timestamp")
    )
    
    # Cleansing
    cleansed = (
        parsed
            .withColumn("transaction_date", to_date(col("transaction_timestamp")))
            .withColumn("transaction_type", upper(trim(col("transaction_type"))))
            .withColumn("status", upper(trim(col("status"))))
            .withColumn("silver_processed_timestamp", current_timestamp())
            .dropDuplicates(["transaction_id"])
    )
    
    return cleansed

# =============================================================================
# SILVER TABLE: QUARANTINE FOR INVALID RECORDS
# =============================================================================

@dlt.table(
    name="silver_events_quarantine",
    comment="Invalid events that failed quality checks"
)
def silver_events_quarantine():
    """
    Capture records that failed expectations for investigation.
    """
    
    bronze_df = dlt.read_stream("bronze_kafka_events_parsed")
    
    return (
        bronze_df
            # Apply same type casting
            .withColumn("event_id", trim(col("event_id")))
            .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
            
            # Filter for invalid records
            .filter(
                col("event_id").isNull() |
                (col("event_id") == "") |
                col("event_timestamp").isNull() |
                col("customer_id").isNull()
            )
            
            # Add failure reason
            .withColumn("failure_reason",
                when(col("event_id").isNull() | (col("event_id") == ""), "Missing event_id")
                .when(col("event_timestamp").isNull(), "Missing event_timestamp")
                .when(col("customer_id").isNull(), "Missing customer_id")
                .otherwise("Unknown")
            )
            .withColumn("quarantine_timestamp", current_timestamp())
    )
```

---

## 4. Silver Pipeline - API Data

### 4.1 Customer Data Cleansing

**File:** `pipelines/silver_api_cleansing.py`

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
# SILVER TABLE: CUSTOMERS (SCD Type 2)
# =============================================================================

@dlt.table(
    name="silver_customers_scd2",
    comment="Customer dimension with SCD Type 2 for history tracking",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'")
def silver_customers_scd2():
    """
    Implement SCD Type 2 for customer dimension.
    
    Columns:
    - customer_id: Business key
    - customer_name, email, etc: Attributes
    - effective_date: When this version became active
    - end_date: When this version was superseded (NULL for current)
    - is_current: Flag for current record
    """
    
    # Read from Bronze
    bronze_customers = dlt.read_stream("bronze_customer_api_raw")
    
    # Cleanse and standardize
    cleansed = (
        bronze_customers
            .withColumn("customer_id", trim(col("customer_id")))
            .withColumn("customer_name", trim(col("name")))
            .withColumn("email", lower(trim(col("email"))))
            .withColumn("phone", regexp_replace(col("phone"), r"[^0-9]", ""))
            .withColumn("country_code", upper(trim(col("country"))))
            
            # Extract nested address if present
            .withColumn("address_line1", col("address.line1"))
            .withColumn("address_city", col("address.city"))
            .withColumn("address_state", col("address.state"))
            .withColumn("address_zip", col("address.zip"))
            
            # Add effective dating for SCD2
            .withColumn("effective_date", current_timestamp())
            .withColumn("end_date", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .withColumn("silver_processed_timestamp", current_timestamp())
            
            # Select final schema
            .select(
                "customer_id",
                "customer_name",
                "email",
                "phone",
                "country_code",
                "address_line1",
                "address_city",
                "address_state",
                "address_zip",
                "effective_date",
                "end_date",
                "is_current",
                "silver_processed_timestamp"
            )
    )
    
    return cleansed

# =============================================================================
# SILVER TABLE: PRODUCTS
# =============================================================================

@dlt.table(
    name="silver_products_clean",
    comment="Cleansed product reference data",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("valid_price", "unit_price >= 0")
def silver_products_clean():
    """
    Cleanse product reference data from API.
    """
    
    bronze_products = dlt.read_stream("bronze_product_api_raw")
    
    return (
        bronze_products
            .withColumn("product_id", trim(col("product_id")))
            .withColumn("product_name", trim(col("name")))
            .withColumn("category", upper(trim(col("category"))))
            .withColumn("unit_price", col("price").cast("decimal(18,2)"))
            .withColumn("is_active", col("active").cast("boolean"))
            .withColumn("silver_processed_timestamp", current_timestamp())
            .dropDuplicates(["product_id"])
            .select(
                "product_id",
                "product_name",
                "category",
                "unit_price",
                "is_active",
                "silver_processed_timestamp"
            )
    )
```

---

## 5. Silver Pipeline - SFTP Files

### 5.1 Reconciliation File Cleansing

**File:** `pipelines/silver_sftp_cleansing.py`

```python
import dlt
from pyspark.sql.functions import *

# =============================================================================
# SILVER TABLE: PARTNER RECONCILIATION
# =============================================================================

@dlt.table(
    name="silver_partner_recon_clean",
    comment="Cleansed partner reconciliation data",
    table_properties={
        "quality": "silver"
    },
    partition_cols=["recon_date"]
)
@dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "transaction_date IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount != 0")
@dlt.expect("valid_currency", "currency IN ('USD', 'EUR', 'GBP', 'CAD')")
def silver_partner_recon_clean():
    """
    Cleanse reconciliation files from partner SFTP.
    """
    
    bronze_recon = dlt.read_stream("bronze_partner_recon_raw")
    
    return (
        bronze_recon
            # Type casting
            .withColumn("transaction_id", trim(col("transaction_id")))
            .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
            .withColumn("amount", col("amount").cast("decimal(18,2)"))
            .withColumn("currency", upper(trim(col("currency"))))
            .withColumn("status", upper(trim(col("status"))))
            
            # Extract recon date from filename
            .withColumn("recon_date", to_date(col("file_date"), "yyyy-MM-dd"))
            
            # Currency conversion to USD (simplified example)
            .withColumn("amount_usd",
                when(col("currency") == "USD", col("amount"))
                .when(col("currency") == "EUR", col("amount") * 1.1)  # Use actual exchange rates
                .when(col("currency") == "GBP", col("amount") * 1.3)
                .when(col("currency") == "CAD", col("amount") * 0.75)
                .otherwise(col("amount"))
            )
            
            .withColumn("silver_processed_timestamp", current_timestamp())
            
            # Deduplication
            .dropDuplicates(["transaction_id", "transaction_date"])
            
            .select(
                "transaction_id",
                "transaction_date",
                "amount",
                "currency",
                "amount_usd",
                "status",
                "recon_date",
                "silver_processed_timestamp"
            )
    )
```

---

## 6. Advanced Patterns

### 6.1 Slowly Changing Dimension (SCD) Type 2 Full Example

```python
@dlt.table(
    name="silver_customers_scd2_merge",
    comment="Customer SCD Type 2 with MERGE logic"
)
def silver_customers_scd2_merge():
    """
    Full SCD Type 2 implementation using MERGE.
    Handles inserts and updates properly.
    """
    
    # This would typically use APPLY CHANGES INTO
    # DLT's built-in CDC functionality
    pass

# Better approach: Use DLT's APPLY CHANGES
@dlt.view
def customer_changes():
    """Staged customer changes from API"""
    return (
        dlt.read_stream("bronze_customer_api_raw")
            .select(
                "customer_id",
                col("name").alias("customer_name"),
                "email",
                "phone",
                col("updated_at").alias("__timestamp")
            )
    )

dlt.apply_changes(
    target="silver_customers_scd2",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by="__timestamp",
    stored_as_scd_type="2"
)
```

### 6.2 Deduplication with Window Functions

```python
@dlt.table(
    name="silver_events_deduplicated",
    comment="Events deduplicated using latest timestamp"
)
def silver_events_deduplicated():
    """
    Deduplicate using window functions to keep latest record.
    """
    
    bronze_df = dlt.read_stream("bronze_kafka_events_parsed")
    
    # Define window partitioned by event_id, ordered by timestamp
    window_spec = Window.partitionBy("event_id").orderBy(col("event_timestamp").desc())
    
    return (
        bronze_df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
    )
```

### 6.3 Complex Data Quality Rules

```python
@dlt.table(
    name="silver_transactions_validated",
    comment="Transactions with complex validation rules"
)
@dlt.expect_all({
    "valid_amount_range": "transaction_amount BETWEEN 0.01 AND 1000000",
    "valid_timestamp_range": "transaction_timestamp BETWEEN current_date() - INTERVAL 1 YEAR AND current_timestamp()",
    "valid_status": "status IN ('COMPLETED', 'PENDING', 'FAILED')",
    "merchant_required_for_purchase": "transaction_type != 'PURCHASE' OR merchant_id IS NOT NULL"
})
@dlt.expect_or_fail("critical_fraud_check", "NOT (transaction_amount > 10000 AND customer_id IS NULL)")
def silver_transactions_validated():
    """
    Apply multiple complex validation rules.
    """
    return dlt.read_stream("bronze_transaction_events_raw")
```

---

## 7. Data Quality Monitoring

### 7.1 Create Quality Metrics Table

```python
@dlt.table(
    name="silver_quality_metrics",
    comment="Track data quality metrics over time"
)
def silver_quality_metrics():
    """
    Aggregate data quality metrics from event logs.
    """
    return (
        spark.sql("""
            SELECT
                timestamp,
                details:flow_definition.output_dataset as table_name,
                details:flow_definition.schema as schema_name,
                details:flow_progress.metrics.num_output_rows as rows_processed,
                details:flow_progress.data_quality.dropped_records as rows_dropped,
                details:flow_progress.data_quality.expectations as quality_checks
            FROM event_log('<pipeline-id>')
            WHERE event_type = 'flow_progress'
        """)
    )
```

### 7.2 Query Quality Metrics

```sql
-- Check expectation failure rates
SELECT
    table_name,
    SUM(rows_dropped) as total_dropped,
    SUM(rows_processed) as total_processed,
    ROUND(100.0 * SUM(rows_dropped) / NULLIF(SUM(rows_processed), 0), 2) as drop_rate_pct
FROM dev_data_platform.silver_cleansed.silver_quality_metrics
WHERE date(timestamp) = current_date()
GROUP BY table_name
ORDER BY drop_rate_pct DESC;
```

---

## 8. Running Silver Pipelines

### 8.1 Create Combined Silver Pipeline

```python
# File: pipelines/silver_all.py

# Import all Silver transformations
exec(open("silver_kafka_cleansing.py").read())
exec(open("silver_api_cleansing.py").read())
exec(open("silver_sftp_cleansing.py").read())
```

### 8.2 DLT Pipeline Configuration

**Create Pipeline in Databricks:**
- **Name:** `silver-data-cleansing`
- **Notebook Libraries:** `pipelines/silver_all.py`
- **Target:** `dev_data_platform.silver_cleansed`
- **Pipeline Mode:** Continuous (inherits from Bronze streaming)
- **Cluster:** Autoscaling 2-8 workers

---

## 9. Testing & Validation

### 9.1 Row Count Validation

```sql
-- Compare Bronze vs Silver row counts
SELECT 
    'Bronze' as layer,
    COUNT(*) as row_count
FROM dev_data_platform.bronze_raw.bronze_kafka_events_parsed
WHERE ingestion_date = current_date()

UNION ALL

SELECT 
    'Silver' as layer,
    COUNT(*) as row_count
FROM dev_data_platform.silver_cleansed.silver_events_clean
WHERE event_date = current_date();
```

### 9.2 Data Quality Dashboard

```sql
-- Quality summary
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT event_type) as event_types,
    MIN(event_timestamp) as earliest_event,
    MAX(event_timestamp) as latest_event,
    AVG(amount) as avg_amount
FROM dev_data_platform.silver_cleansed.silver_events_clean
WHERE event_date = current_date();
```

---

## 10. Best Practices

✅ **Use expectations liberally** - Better to catch issues early  
✅ **Create quarantine tables** - Investigate failed records  
✅ **Implement SCD Type 2** - For dimensional data needing history  
✅ **Deduplicate early** - Before expensive transformations  
✅ **Monitor quality metrics** - Track drop rates and failures  
✅ **Partition by business date** - Not ingestion date  
✅ **Z-order frequently filtered columns** - For better performance  

---

## Next Steps

Silver layer complete! Now create business aggregations:

→ **[05-Claims-Fragment-Assembly.md](05-Claims-Fragment-Assembly.md)** - Assemble claims fragments (if applicable)

---

**References:**
- ← [03-Bronze-Layer.md](03-Bronze-Layer.md)
- → [05-Claims-Fragment-Assembly.md](05-Claims-Fragment-Assembly.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
