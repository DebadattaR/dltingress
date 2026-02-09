# 07 - Security & Governance

**Related Documents:**
- ← [06-Gold-Layer.md](06-Gold-Layer.md) - Gold Layer Implementation
- → [08-Monitoring-Alerting.md](08-Monitoring-Alerting.md) - Monitoring & Alerting
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview

---

## 1. Unity Catalog Security Model

### 1.1 Security Hierarchy

```
Azure AD/Entra ID
    ↓
Unity Catalog Metastore
    ↓
Catalogs (dev, test, prod)
    ↓
Schemas (bronze_raw, silver_cleansed, gold_analytics)
    ↓
Tables/Views
    ↓
Columns (optional masking/encryption)
```

### 1.2 Key Concepts

- **GRANT/REVOKE**: SQL-based permission management
- **Principals**: Users, service principals, groups
- **Privileges**: USE, SELECT, MODIFY, CREATE, etc.
- **Securable Objects**: Catalogs, schemas, tables, views

---

## 2. Group-Based Access Control

### 2.1 Create Security Groups

```sql
-- Create groups in Unity Catalog
CREATE GROUP IF NOT EXISTS data_engineers
COMMENT 'Data engineering team - build and maintain pipelines';

CREATE GROUP IF NOT EXISTS data_analysts
COMMENT 'Analytics team - read-only access to Gold layer';

CREATE GROUP IF NOT EXISTS data_scientists
COMMENT 'Data science team - ML and advanced analytics';

CREATE GROUP IF NOT EXISTS business_users
COMMENT 'Business users - dashboards and reports';

CREATE GROUP IF NOT EXISTS platform_admins
COMMENT 'Platform administrators - full access';
```

### 2.2 Add Users to Groups

```sql
-- Add users to groups
ALTER GROUP data_engineers ADD USER 'john.doe@company.com';
ALTER GROUP data_analysts ADD USER 'jane.smith@company.com';
ALTER GROUP platform_admins ADD USER 'admin@company.com';

-- Verify group membership
SHOW GROUPS;
SHOW GROUP MEMBERS data_engineers;
```

---

## 3. Catalog-Level Permissions

### 3.1 Development Environment

```sql
-- Data Engineers: Full access to dev catalog
GRANT USE CATALOG ON CATALOG dev_data_platform TO data_engineers;
GRANT CREATE SCHEMA ON CATALOG dev_data_platform TO data_engineers;
GRANT USE SCHEMA ON CATALOG dev_data_platform TO data_engineers;
GRANT SELECT, MODIFY ON CATALOG dev_data_platform TO data_engineers;

-- Data Analysts: Read-only on gold schema
GRANT USE CATALOG ON CATALOG dev_data_platform TO data_analysts;
GRANT USE SCHEMA ON SCHEMA dev_data_platform.gold_analytics TO data_analysts;
GRANT SELECT ON SCHEMA dev_data_platform.gold_analytics TO data_analysts;
```

### 3.2 Production Environment

```sql
-- Data Engineers: Read-only in production
GRANT USE CATALOG ON CATALOG prod_data_platform TO data_engineers;
GRANT SELECT ON CATALOG prod_data_platform TO data_engineers;

-- Data Analysts: Read access to Gold layer
GRANT USE CATALOG ON CATALOG prod_data_platform TO data_analysts;
GRANT USE SCHEMA ON SCHEMA prod_data_platform.gold_analytics TO data_analysts;
GRANT SELECT ON SCHEMA prod_data_platform.gold_analytics TO data_analysts;

-- Business Users: Specific Gold tables only
GRANT USE CATALOG ON CATALOG prod_data_platform TO business_users;
GRANT USE SCHEMA ON SCHEMA prod_data_platform.gold_analytics TO business_users;
GRANT SELECT ON TABLE prod_data_platform.gold_analytics.gold_customer_360 TO business_users;
GRANT SELECT ON TABLE prod_data_platform.gold_analytics.gold_daily_transaction_metrics TO business_users;

-- Platform Admins: Full access
GRANT ALL PRIVILEGES ON CATALOG prod_data_platform TO platform_admins;
```

---

## 4. Table-Level Security

### 4.1 Fine-Grained Permissions

```sql
-- Grant access to specific tables
GRANT SELECT ON TABLE prod_data_platform.gold_analytics.gold_dim_customers TO data_analysts;
GRANT SELECT ON TABLE prod_data_platform.gold_analytics.gold_fact_transactions TO data_analysts;

-- Revoke access if needed
REVOKE SELECT ON TABLE prod_data_platform.silver_cleansed.silver_customers_scd2 FROM business_users;
```

### 4.2 View Current Permissions

```sql
-- Show grants on a specific table
SHOW GRANTS ON TABLE prod_data_platform.gold_analytics.gold_customer_360;

-- Show grants for a specific user
SHOW GRANTS ON CATALOG prod_data_platform FOR USER 'jane.smith@company.com';
```

---

## 5. Column-Level Security

### 5.1 Dynamic Views for PII Masking

```sql
-- Create view with masked sensitive data
CREATE OR REPLACE VIEW prod_data_platform.gold_analytics.gold_customers_masked AS
SELECT
    customer_id,
    full_name,
    -- Mask email - show domain only
    CONCAT('***@', SPLIT(email, '@')[1]) AS email,
    -- Mask phone - show last 4 digits
    CONCAT('XXX-XXX-', RIGHT(phone, 4)) AS phone,
    country_code,
    region,
    customer_segment,
    customer_status
FROM prod_data_platform.gold_analytics.gold_dim_customers;

-- Grant access to masked view for business users
GRANT SELECT ON VIEW prod_data_platform.gold_analytics.gold_customers_masked TO business_users;
```

### 5.2 Row-Level Security

```sql
-- Create region-specific view
CREATE OR REPLACE VIEW prod_data_platform.gold_analytics.gold_customers_us_only AS
SELECT *
FROM prod_data_platform.gold_analytics.gold_dim_customers
WHERE country_code = 'US';

-- Grant access to specific regions
GRANT SELECT ON VIEW prod_data_platform.gold_analytics.gold_customers_us_only TO us_analytics_team;
```

---

## 6. Data Encryption

### 6.1 Encryption at Rest

**Azure Storage Service Encryption (SSE):**
- Automatically enabled on ADLS Gen2
- Uses Microsoft-managed keys by default
- Optional: Customer-managed keys (CMK) via Azure Key Vault

```bash
# Enable customer-managed encryption key
az storage account update \
  --name stdataplatformprod \
  --resource-group rg-data-platform-prod \
  --encryption-key-source Microsoft.Keyvault \
  --encryption-key-vault https://kv-dataplatform-prod.vault.azure.net/ \
  --encryption-key-name storage-encryption-key
```

### 6.2 Encryption in Transit

- **TLS 1.2+** enforced for all connections
- **Event Hubs**: Encrypted connections only
- **Databricks**: Encrypted communication between nodes

```python
# Event Hubs configuration enforces encryption
eventhub_config = {
    "eventhubs.connectionString": encrypted_connection_string,
    # TLS is enforced by default
}
```

### 6.3 Column-Level Encryption (Optional)

```python
# Example: Encrypt sensitive columns before writing
from pyspark.sql.functions import sha2, base64

@dlt.table(name="silver_customers_encrypted")
def encrypt_sensitive_data():
    customers = dlt.read("bronze_customer_api_raw")
    
    return (
        customers
            # Hash email for privacy
            .withColumn("email_hash", sha2(col("email"), 256))
            # Encrypt credit card (if applicable)
            .withColumn("encrypted_cc", base64(col("credit_card")))
            .drop("email", "credit_card")
    )
```

---

## 7. Audit Logging

### 7.1 Unity Catalog Audit Logs

Unity Catalog automatically logs all access and modification events.

**Query Audit Logs:**
```sql
-- View recent access to sensitive tables
SELECT
    event_time,
    user_identity.email AS user_email,
    request_params.full_name_arg AS table_name,
    action_name,
    response.status_code
FROM system.access.audit
WHERE action_name = 'getTable'
  AND request_params.full_name_arg LIKE '%customer%'
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC
LIMIT 100;
```

### 7.2 Delta Lake Change Data Feed

Enable CDC for tracking all changes:

```sql
-- Enable Change Data Feed on tables
ALTER TABLE prod_data_platform.silver_cleansed.silver_customers_scd2
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Query changes
SELECT *
FROM table_changes('prod_data_platform.silver_cleansed.silver_customers_scd2', 0)
WHERE _change_type IN ('insert', 'update_postimage', 'delete')
ORDER BY _commit_timestamp DESC;
```

### 7.3 Azure Monitor Integration

```python
# Send custom audit events to Azure Monitor
import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler

logger = logging.getLogger(__name__)
logger.addHandler(AzureLogHandler(
    connection_string='InstrumentationKey=<key>'
))

# Log data access
logger.info('Data accessed', extra={
    'custom_dimensions': {
        'user': dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get(),
        'table': 'gold_customer_360',
        'action': 'SELECT',
        'timestamp': str(datetime.now())
    }
})
```

---

## 8. Compliance & Governance

### 8.1 Data Retention Policies

```sql
-- Set retention on Bronze tables (30 days)
ALTER TABLE dev_data_platform.bronze_raw.bronze_kafka_events_raw
SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 30 days'
);

-- Vacuum to enforce retention
VACUUM dev_data_platform.bronze_raw.bronze_kafka_events_raw RETAIN 720 HOURS;
```

### 8.2 GDPR Compliance - Right to be Forgotten

```sql
-- Delete customer data for GDPR requests
DELETE FROM prod_data_platform.silver_cleansed.silver_customers_scd2
WHERE customer_id = 'CUST-12345';

DELETE FROM prod_data_platform.gold_analytics.gold_fact_transactions
WHERE customer_id = 'CUST-12345';

-- Vacuum to permanently remove
VACUUM prod_data_platform.silver_cleansed.silver_customers_scd2 RETAIN 0 HOURS;
```

### 8.3 Data Lineage

Unity Catalog automatically tracks lineage:

```sql
-- View lineage for a table
DESCRIBE HISTORY prod_data_platform.gold_analytics.gold_customer_360;

-- View upstream and downstream dependencies
-- Available in Databricks UI: Data Explorer → Table → Lineage tab
```

---

## 9. Service Principal Authentication

### 9.1 Create Service Principal

```bash
# Azure CLI
az ad sp create-for-rbac \
  --name sp-databricks-pipeline \
  --role Contributor \
  --scopes /subscriptions/<subscription-id>/resourceGroups/rg-data-platform-prod

# Output:
# {
#   "appId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
#   "password": "xxxxxxxx",
#   "tenant": "xxxxxxxx"
# }
```

### 9.2 Grant Service Principal Access

```sql
-- Create service principal in Unity Catalog
CREATE SERVICE PRINCIPAL IF NOT EXISTS '{{ sp_application_id }}';

-- Grant permissions for automated pipelines
GRANT USE CATALOG ON CATALOG prod_data_platform TO SERVICE PRINCIPAL '{{ sp_application_id }}';
GRANT SELECT, MODIFY ON CATALOG prod_data_platform TO SERVICE PRINCIPAL '{{ sp_application_id }}';
```

### 9.3 Use Service Principal in Pipelines

```python
# Configure in DLT pipeline
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get("key-vault-scope", "sp-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get("key-vault-scope", "sp-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
```

---

## 10. Security Best Practices

### 10.1 Secret Management

```bash
# Store all secrets in Azure Key Vault
az keyvault secret set \
  --vault-name kv-dataplatform-prod \
  --name eventhub-connection-string \
  --value "Endpoint=sb://..."

# Access in Databricks
connection_string = dbutils.secrets.get(
    scope="key-vault-scope",
    key="eventhub-connection-string"
)
```

### 10.2 Network Isolation

- **No public internet access** to storage accounts (private endpoints)
- **VNet injection** for Databricks workspaces
- **NSG rules** to restrict traffic
- **Private DNS zones** for name resolution

### 10.3 Least Privilege Principle

```sql
-- Start with minimal permissions
GRANT USE CATALOG ON CATALOG prod_data_platform TO new_user;

-- Grant additional permissions only as needed
GRANT USE SCHEMA ON SCHEMA prod_data_platform.gold_analytics TO new_user;
GRANT SELECT ON TABLE prod_data_platform.gold_analytics.gold_daily_metrics TO new_user;
```

### 10.4 Regular Access Reviews

```sql
-- Audit user permissions quarterly
SELECT
    grantee,
    object_type,
    object_key,
    privilege
FROM system.information_schema.grants
WHERE catalog_name = 'prod_data_platform'
ORDER BY grantee, object_type;
```

---

## 11. Incident Response

### 11.1 Unauthorized Access Detection

```sql
-- Query audit logs for suspicious activity
SELECT
    event_time,
    user_identity.email,
    action_name,
    request_params.full_name_arg AS accessed_object,
    response.status_code
FROM system.access.audit
WHERE response.status_code = 403  -- Access denied
  AND event_date >= current_date() - INTERVAL 1 DAY
ORDER BY event_time DESC;
```

### 11.2 Emergency Access Revocation

```sql
-- Immediately revoke all access for a user
REVOKE ALL PRIVILEGES ON CATALOG prod_data_platform FROM USER 'compromised.user@company.com';

-- Remove from all groups
ALTER GROUP data_engineers DROP USER 'compromised.user@company.com';
ALTER GROUP data_analysts DROP USER 'compromised.user@company.com';
```

---

## 12. Compliance Checklist

- [ ] All catalogs have appropriate GRANT statements
- [ ] Sensitive data masked in views for business users
- [ ] Audit logging enabled (automatic in Unity Catalog)
- [ ] Change Data Feed enabled on critical tables
- [ ] Encryption at rest and in transit enabled
- [ ] Secrets stored in Azure Key Vault
- [ ] Service principals used for automated pipelines
- [ ] Regular access reviews scheduled
- [ ] Data retention policies defined and enforced
- [ ] GDPR deletion procedures documented
- [ ] Network isolation configured (private endpoints)
- [ ] Incident response plan documented

---

## Next Steps

Security configured! Now set up monitoring:

→ **[08-Monitoring-Alerting.md](08-Monitoring-Alerting.md)** - Implement observability and alerting

---

**References:**
- ← [06-Gold-Layer.md](06-Gold-Layer.md)
- → [08-Monitoring-Alerting.md](08-Monitoring-Alerting.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
- [Unity Catalog Security Guide](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/)
