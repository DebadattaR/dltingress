# 06 - Gold Layer Implementation

**Related Documents:**
- ← [05-Claims-Fragment-Assembly.md](05-Claims-Fragment-Assembly.md) - Claims Fragment Assembly Pattern
- → [07-Security-Governance.md](07-Security-Governance.md) - Security & Governance
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview

---

## 1. Gold Layer Overview

### 1.1 Purpose
The Gold layer provides business-ready, optimized datasets for:
- **Analytics and BI reporting**
- **Machine learning features**
- **Executive dashboards**
- **Self-service analytics**

### 1.2 Design Principles
- ✅ Business logic applied
- ✅ Dimensional modeling (facts & dimensions)
- ✅ Pre-aggregated metrics
- ✅ Optimized for read performance
- ✅ Denormalized where appropriate

---

## 2. Create Gold Schema

```sql
-- Run in Databricks SQL or notebook

USE CATALOG dev_data_platform;

CREATE SCHEMA IF NOT EXISTS gold_analytics
COMMENT 'Business analytics and reporting layer'
LOCATION 'abfss://data@<storage_account>.dfs.core.windows.net/dev/gold/';
```

---

## 3. Fact Tables

### 3.1 Fact: Transactions

**File:** `pipelines/gold_facts.py`

```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =============================================================================
# GOLD TABLE: FACT TRANSACTIONS
# =============================================================================

@dlt.table(
    name="gold_fact_transactions",
    comment="Transaction fact table optimized for analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "customer_id,transaction_date"
    },
    partition_cols=["transaction_year", "transaction_month"]
)
def gold_fact_transactions():
    """
    Create fact table for transactions with all necessary dimensions.
    
    Schema follows Kimball dimensional modeling:
    - Foreign keys to dimension tables
    - Measures (amounts, counts, etc.)
    - Degenerate dimensions (transaction_id)
    - Date/time fields for slicing
    """
    
    # Read from Silver
    transactions = dlt.read("silver_transactions_enriched")
    
    return (
        transactions
            # Add surrogate keys and business keys
            .withColumn("transaction_key", monotonically_increasing_id())
            
            # Extract date parts for partitioning
            .withColumn("transaction_year", year(col("transaction_date")))
            .withColumn("transaction_month", month(col("transaction_date")))
            .withColumn("transaction_day", dayofmonth(col("transaction_date")))
            .withColumn("transaction_hour", hour(col("transaction_timestamp")))
            .withColumn("day_of_week", dayofweek(col("transaction_date")))
            .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False))
            
            # Business categorization
            .withColumn("transaction_category",
                when(col("transaction_amount") < 50, "Small")
                .when(col("transaction_amount") < 500, "Medium")
                .when(col("transaction_amount") < 5000, "Large")
                .otherwise("Extra Large")
            )
            
            # Add gold processing timestamp
            .withColumn("gold_processed_timestamp", current_timestamp())
            
            # Select final schema
            .select(
                "transaction_key",
                "transaction_id",
                "customer_id",
                "merchant_id",
                "transaction_timestamp",
                "transaction_date",
                "transaction_year",
                "transaction_month",
                "transaction_day",
                "transaction_hour",
                "day_of_week",
                "is_weekend",
                "transaction_type",
                "transaction_amount",
                "transaction_category",
                "status",
                "gold_processed_timestamp"
            )
    )

# =============================================================================
# GOLD TABLE: FACT EVENTS
# =============================================================================

@dlt.table(
    name="gold_fact_events",
    comment="Event fact table for clickstream and behavioral analysis",
    table_properties={
        "quality": "gold"
    },
    partition_cols=["event_year", "event_month"]
)
def gold_fact_events():
    """
    Event fact table for analytics.
    """
    
    events = dlt.read("silver_events_clean")
    
    return (
        events
            .withColumn("event_key", monotonically_increasing_id())
            .withColumn("event_year", year(col("event_date")))
            .withColumn("event_month", month(col("event_date")))
            .withColumn("event_hour", hour(col("event_timestamp")))
            .withColumn("gold_processed_timestamp", current_timestamp())
            .select(
                "event_key",
                "event_id",
                "event_type",
                "event_timestamp",
                "event_date",
                "event_year",
                "event_month",
                "event_hour",
                "customer_id",
                "amount",
                "gold_processed_timestamp"
            )
    )
```

---

## 4. Dimension Tables

### 4.1 Dimension: Customers

**File:** `pipelines/gold_dimensions.py`

```python
import dlt
from pyspark.sql.functions import *

# =============================================================================
# GOLD TABLE: DIM CUSTOMERS
# =============================================================================

@dlt.table(
    name="gold_dim_customers",
    comment="Customer dimension table (current records only)",
    table_properties={
        "quality": "gold"
    }
)
def gold_dim_customers():
    """
    Customer dimension - current snapshot from SCD Type 2 table.
    """
    
    # Read from Silver SCD2 table
    customers_scd2 = dlt.read("silver_customers_scd2")
    
    return (
        customers_scd2
            # Get only current records
            .filter(col("is_current") == True)
            
            # Add derived attributes
            .withColumn("full_name", col("customer_name"))
            .withColumn("email_domain", 
                regexp_extract(col("email"), r"@(.+)$", 1)
            )
            
            # Categorize by country
            .withColumn("region",
                when(col("country_code").isin(["US", "CA", "MX"]), "North America")
                .when(col("country_code").isin(["UK", "DE", "FR", "ES", "IT"]), "Europe")
                .otherwise("Other")
            )
            
            .withColumn("gold_processed_timestamp", current_timestamp())
            
            .select(
                "customer_id",
                "full_name",
                "email",
                "email_domain",
                "phone",
                "country_code",
                "region",
                "address_line1",
                "address_city",
                "address_state",
                "address_zip",
                "effective_date",
                "gold_processed_timestamp"
            )
    )

# =============================================================================
# GOLD TABLE: DIM PRODUCTS
# =============================================================================

@dlt.table(
    name="gold_dim_products",
    comment="Product dimension table",
    table_properties={
        "quality": "gold"
    }
)
def gold_dim_products():
    """
    Product dimension with categorization.
    """
    
    products = dlt.read("silver_products_clean")
    
    return (
        products
            # Only active products
            .filter(col("is_active") == True)
            
            # Price categorization
            .withColumn("price_tier",
                when(col("unit_price") < 20, "Budget")
                .when(col("unit_price") < 100, "Standard")
                .when(col("unit_price") < 500, "Premium")
                .otherwise("Luxury")
            )
            
            .withColumn("gold_processed_timestamp", current_timestamp())
            
            .select(
                "product_id",
                "product_name",
                "category",
                "unit_price",
                "price_tier",
                "gold_processed_timestamp"
            )
    )

# =============================================================================
# GOLD TABLE: DIM DATE
# =============================================================================

@dlt.table(
    name="gold_dim_date",
    comment="Date dimension table for analytics"
)
def gold_dim_date():
    """
    Create a comprehensive date dimension.
    Generates dates for 5 years (2 past, current, 2 future).
    """
    
    from datetime import datetime, timedelta
    
    # Generate date range
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2028, 12, 31)
    
    dates = []
    current = start_date
    while current <= end_date:
        dates.append((current,))
        current += timedelta(days=1)
    
    df = spark.createDataFrame(dates, ["date"])
    
    return (
        df
            .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
            .withColumn("year", year(col("date")))
            .withColumn("quarter", quarter(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("month_name", date_format(col("date"), "MMMM"))
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("day_of_week", dayofweek(col("date")))
            .withColumn("day_name", date_format(col("date"), "EEEE"))
            .withColumn("week_of_year", weekofyear(col("date")))
            .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False))
            .withColumn("is_holiday", lit(False))  # Extend with holiday logic
            .withColumn("fiscal_year", 
                when(col("month") >= 7, col("year") + 1).otherwise(col("year"))
            )
            .select(
                "date_key",
                "date",
                "year",
                "quarter",
                "month",
                "month_name",
                "day",
                "day_of_week",
                "day_name",
                "week_of_year",
                "is_weekend",
                "is_holiday",
                "fiscal_year"
            )
    )
```

---

## 5. Aggregated Tables

### 5.1 Daily Metrics

**File:** `pipelines/gold_aggregations.py`

```python
import dlt
from pyspark.sql.functions import *

# =============================================================================
# GOLD TABLE: DAILY TRANSACTION METRICS
# =============================================================================

@dlt.table(
    name="gold_daily_transaction_metrics",
    comment="Daily aggregated transaction metrics",
    table_properties={
        "quality": "gold"
    },
    partition_cols=["metric_year", "metric_month"]
)
def gold_daily_transaction_metrics():
    """
    Daily aggregation of transaction metrics.
    Pre-computed for dashboard performance.
    """
    
    transactions = dlt.read("gold_fact_transactions")
    
    return (
        transactions
            .groupBy("transaction_date", "transaction_type")
            .agg(
                count("*").alias("transaction_count"),
                sum("transaction_amount").alias("total_amount"),
                avg("transaction_amount").alias("avg_amount"),
                min("transaction_amount").alias("min_amount"),
                max("transaction_amount").alias("max_amount"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants")
            )
            
            # Add date parts for partitioning
            .withColumn("metric_year", year(col("transaction_date")))
            .withColumn("metric_month", month(col("transaction_date")))
            .withColumn("gold_processed_timestamp", current_timestamp())
            
            .select(
                "transaction_date",
                "metric_year",
                "metric_month",
                "transaction_type",
                "transaction_count",
                "total_amount",
                "avg_amount",
                "min_amount",
                "max_amount",
                "unique_customers",
                "unique_merchants",
                "gold_processed_timestamp"
            )
    )

# =============================================================================
# GOLD TABLE: CUSTOMER 360 VIEW
# =============================================================================

@dlt.table(
    name="gold_customer_360",
    comment="360-degree customer view with aggregated metrics",
    table_properties={
        "quality": "gold"
    }
)
def gold_customer_360():
    """
    Comprehensive customer profile combining dimensions and aggregated facts.
    """
    
    # Get customer dimension
    customers = dlt.read("gold_dim_customers")
    
    # Aggregate transaction metrics per customer
    transactions = dlt.read("gold_fact_transactions")
    
    customer_txn_metrics = (
        transactions
            .groupBy("customer_id")
            .agg(
                count("*").alias("lifetime_transaction_count"),
                sum("transaction_amount").alias("lifetime_transaction_value"),
                avg("transaction_amount").alias("avg_transaction_amount"),
                max("transaction_date").alias("last_transaction_date"),
                min("transaction_date").alias("first_transaction_date")
            )
    )
    
    # Calculate additional metrics
    customer_metrics = (
        customer_txn_metrics
            .withColumn("days_since_first_transaction",
                datediff(current_date(), col("first_transaction_date"))
            )
            .withColumn("days_since_last_transaction",
                datediff(current_date(), col("last_transaction_date"))
            )
            
            # Customer segmentation
            .withColumn("customer_segment",
                when(col("lifetime_transaction_value") > 10000, "VIP")
                .when(col("lifetime_transaction_value") > 5000, "High Value")
                .when(col("lifetime_transaction_value") > 1000, "Medium Value")
                .otherwise("Low Value")
            )
            
            .withColumn("customer_status",
                when(col("days_since_last_transaction") > 180, "Inactive")
                .when(col("days_since_last_transaction") > 90, "At Risk")
                .otherwise("Active")
            )
    )
    
    # Join with customer dimension
    return (
        customers
            .join(customer_metrics, "customer_id", "left")
            .select(
                "customer_id",
                "full_name",
                "email",
                "email_domain",
                "country_code",
                "region",
                "lifetime_transaction_count",
                "lifetime_transaction_value",
                "avg_transaction_amount",
                "first_transaction_date",
                "last_transaction_date",
                "days_since_first_transaction",
                "days_since_last_transaction",
                "customer_segment",
                "customer_status"
            )
            .withColumn("gold_processed_timestamp", current_timestamp())
    )

# =============================================================================
# GOLD TABLE: HOURLY EVENT COUNTS
# =============================================================================

@dlt.table(
    name="gold_hourly_event_metrics",
    comment="Hourly event metrics for real-time monitoring",
    table_properties={
        "quality": "gold"
    }
)
def gold_hourly_event_metrics():
    """
    Hourly aggregation for near real-time dashboards.
    """
    
    events = dlt.read("gold_fact_events")
    
    return (
        events
            .withColumn("event_hour_timestamp", 
                date_trunc("hour", col("event_timestamp"))
            )
            .groupBy("event_hour_timestamp", "event_type")
            .agg(
                count("*").alias("event_count"),
                sum("amount").alias("total_amount"),
                countDistinct("customer_id").alias("unique_users")
            )
            .withColumn("gold_processed_timestamp", current_timestamp())
    )
```

---

## 6. Business Logic Views

### 6.1 Active Customer Summary

```python
@dlt.view(
    name="gold_view_active_customers",
    comment="View of active customers with recent activity"
)
def gold_view_active_customers():
    """
    Business view showing active customers only.
    """
    
    customer_360 = dlt.read("gold_customer_360")
    
    return (
        customer_360
            .filter(col("customer_status") == "Active")
            .filter(col("days_since_last_transaction") <= 30)
            .select(
                "customer_id",
                "full_name",
                "email",
                "customer_segment",
                "lifetime_transaction_value",
                "last_transaction_date"
            )
    )
```

### 6.2 Revenue Summary

```python
@dlt.view(
    name="gold_view_revenue_summary",
    comment="Revenue summary by time period"
)
def gold_view_revenue_summary():
    """
    Pre-aggregated revenue metrics for executive dashboard.
    """
    
    daily_metrics = dlt.read("gold_daily_transaction_metrics")
    
    return (
        daily_metrics
            .withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM"))
            .groupBy("year_month", "transaction_type")
            .agg(
                sum("total_amount").alias("monthly_revenue"),
                sum("transaction_count").alias("monthly_transaction_count"),
                avg("avg_amount").alias("avg_transaction_amount")
            )
            .orderBy("year_month", "transaction_type")
    )
```

---

## 7. Performance Optimization

### 7.1 Enable Auto-Optimize

```python
# Add to table properties
table_properties={
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}
```

### 7.2 Z-Ordering

```sql
-- Run periodically on Gold tables
OPTIMIZE dev_data_platform.gold_analytics.gold_fact_transactions
ZORDER BY (customer_id, transaction_date);

OPTIMIZE dev_data_platform.gold_analytics.gold_customer_360
ZORDER BY (customer_id, customer_segment);
```

### 7.3 VACUUM Old Files

```sql
-- Remove old files (run weekly)
VACUUM dev_data_platform.gold_analytics.gold_fact_transactions RETAIN 168 HOURS;
```

---

## 8. Running Gold Pipelines

### 8.1 Create Combined Gold Pipeline

```python
# File: pipelines/gold_all.py

# Import all Gold transformations
exec(open("gold_facts.py").read())
exec(open("gold_dimensions.py").read())
exec(open("gold_aggregations.py").read())
```

### 8.2 DLT Pipeline Configuration

**Create Pipeline in Databricks:**
- **Name:** `gold-analytics-aggregations`
- **Notebook Libraries:** `pipelines/gold_all.py`
- **Target:** `dev_data_platform.gold_analytics`
- **Pipeline Mode:** Triggered (daily refresh) or Continuous
- **Cluster:** Autoscaling 2-6 workers

---

## 9. Validation Queries

### 9.1 Verify Row Counts

```sql
-- Check all Gold tables
SELECT 
    'gold_fact_transactions' as table_name,
    COUNT(*) as row_count
FROM dev_data_platform.gold_analytics.gold_fact_transactions

UNION ALL

SELECT 
    'gold_dim_customers',
    COUNT(*)
FROM dev_data_platform.gold_analytics.gold_dim_customers

UNION ALL

SELECT 
    'gold_customer_360',
    COUNT(*)
FROM dev_data_platform.gold_analytics.gold_customer_360;
```

### 9.2 Business Metrics Validation

```sql
-- Daily revenue trend
SELECT 
    transaction_date,
    transaction_type,
    total_amount,
    transaction_count
FROM dev_data_platform.gold_analytics.gold_daily_transaction_metrics
WHERE transaction_date >= current_date() - INTERVAL 7 DAYS
ORDER BY transaction_date DESC;
```

---

## 10. Best Practices

✅ **Denormalize for read performance** - Pre-join dimensions where needed  
✅ **Create comprehensive date dimension** - Enables powerful time analysis  
✅ **Use Z-ordering** - On frequently filtered columns  
✅ **Implement customer 360 views** - Single source for customer insights  
✅ **Pre-aggregate metrics** - For dashboard performance  
✅ **Create business logic views** - Abstract complexity from end users  
✅ **Partition large fact tables** - By year/month for pruning  

---

## Next Steps

Gold layer complete! Now secure your data:

→ **[07-Security-Governance.md](07-Security-Governance.md)** - Implement security and governance

---

**References:**
- ← [05-Claims-Fragment-Assembly.md](05-Claims-Fragment-Assembly.md)
- → [07-Security-Governance.md](07-Security-Governance.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
