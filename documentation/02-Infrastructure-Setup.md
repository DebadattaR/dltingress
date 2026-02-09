# 02 - Infrastructure Setup

**Related Documents:**
- ← [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview
- → [03-Bronze-Layer.md](03-Bronze-Layer.md) - Bronze Layer Implementation
- ↑ [00-Index.md](00-Index.md) - Documentation Index

---

## 1. Azure Resources Provisioning

### 1.1 Resource Group

```bash
# Azure CLI
RESOURCE_GROUP="rg-data-platform-prod"
LOCATION="eastus2"

az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION
```

### 1.2 Storage Account (ADLS Gen2)

```bash
STORAGE_ACCOUNT="stdataplatformprod"

# Create storage account with hierarchical namespace
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_GRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --access-tier Hot

# Create containers
az storage container create --name data --account-name $STORAGE_ACCOUNT
az storage container create --name landing-zones --account-name $STORAGE_ACCOUNT
az storage container create --name schemas --account-name $STORAGE_ACCOUNT
```

### 1.3 Azure Databricks Workspace

```bash
DATABRICKS_WORKSPACE="dbw-data-platform-prod"

az databricks workspace create \
  --resource-group $RESOURCE_GROUP \
  --name $DATABRICKS_WORKSPACE \
  --location $LOCATION \
  --sku premium \
  --enable-no-public-ip true
```

### 1.4 Azure Event Hubs Namespace

```bash
EVENTHUB_NAMESPACE="evhns-data-platform-prod"

az eventhubs namespace create \
  --resource-group $RESOURCE_GROUP \
  --name $EVENTHUB_NAMESPACE \
  --location $LOCATION \
  --sku Standard \
  --enable-auto-inflate true \
  --maximum-throughput-units 20

# Create event hubs
az eventhubs eventhub create \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENTHUB_NAMESPACE \
  --name customer-events \
  --partition-count 8 \
  --retention-time 7

az eventhubs eventhub create \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENTHUB_NAMESPACE \
  --name transaction-events \
  --partition-count 16 \
  --retention-time 7

# Create consumer group
az eventhubs eventhub consumer-group create \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENTHUB_NAMESPACE \
  --eventhub-name customer-events \
  --name databricks-lakeflow
```

### 1.5 Azure Key Vault

```bash
KEY_VAULT_NAME="kv-dataplatform-prod"

az keyvault create \
  --name $KEY_VAULT_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --enable-rbac-authorization true
```

### 1.6 Azure Data Factory

```bash
ADF_NAME="adf-data-platform-prod"

az datafactory create \
  --resource-group $RESOURCE_GROUP \
  --factory-name $ADF_NAME \
  --location $LOCATION
```

---

## 2. Unity Catalog Setup

### 2.1 Create Unity Catalog Metastore

```python
# Run in Databricks notebook or via Databricks CLI

# This is typically done once per Azure region
# Requires account admin privileges

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create metastore
metastore = w.metastores.create(
    name="data-platform-metastore-eastus2",
    storage_root="abfss://unity-catalog@stdataplatformprod.dfs.core.windows.net/",
    region="eastus2"
)

# Assign to workspace
w.metastores.assign(
    workspace_id=<workspace_id>,
    metastore_id=metastore.metastore_id,
    default_catalog_name="main"
)
```

### 2.2 Create Catalogs for Each Environment

```sql
-- Run in Databricks SQL or notebook

-- Development Catalog
CREATE CATALOG IF NOT EXISTS dev_data_platform
COMMENT 'Development environment catalog';

-- Test/QA Catalog
CREATE CATALOG IF NOT EXISTS test_data_platform
COMMENT 'Test/QA environment catalog';

-- Production Catalog
CREATE CATALOG IF NOT EXISTS prod_data_platform
COMMENT 'Production environment catalog';
```

### 2.3 Create Schemas

```sql
-- Development schemas
USE CATALOG dev_data_platform;

CREATE SCHEMA IF NOT EXISTS bronze_raw
COMMENT 'Raw data ingestion layer'
LOCATION 'abfss://data@stdataplatformprod.dfs.core.windows.net/dev/bronze/';

CREATE SCHEMA IF NOT EXISTS silver_cleansed
COMMENT 'Cleansed and validated data layer'
LOCATION 'abfss://data@stdataplatformprod.dfs.core.windows.net/dev/silver/';

CREATE SCHEMA IF NOT EXISTS gold_analytics
COMMENT 'Business analytics layer'
LOCATION 'abfss://data@stdataplatformprod.dfs.core.windows.net/dev/gold/';

-- Repeat for test_data_platform and prod_data_platform
```

---

## 3. Network Configuration

### 3.1 Virtual Network Setup

```bash
VNET_NAME="vnet-data-platform"
DATABRICKS_PUBLIC_SUBNET="snet-databricks-public"
DATABRICKS_PRIVATE_SUBNET="snet-databricks-private"

# Create VNet
az network vnet create \
  --resource-group $RESOURCE_GROUP \
  --name $VNET_NAME \
  --address-prefix 10.0.0.0/16 \
  --location $LOCATION

# Create subnets for Databricks
az network vnet subnet create \
  --resource-group $RESOURCE_GROUP \
  --vnet-name $VNET_NAME \
  --name $DATABRICKS_PUBLIC_SUBNET \
  --address-prefix 10.0.1.0/24

az network vnet subnet create \
  --resource-group $RESOURCE_GROUP \
  --vnet-name $VNET_NAME \
  --name $DATABRICKS_PRIVATE_SUBNET \
  --address-prefix 10.0.2.0/24
```

### 3.2 Private Endpoints

```bash
# Private endpoint for Storage Account
az network private-endpoint create \
  --resource-group $RESOURCE_GROUP \
  --name pe-storage \
  --vnet-name $VNET_NAME \
  --subnet $DATABRICKS_PRIVATE_SUBNET \
  --private-connection-resource-id $(az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query id -o tsv) \
  --group-id dfs \
  --connection-name storage-connection

# Private endpoint for Key Vault
az network private-endpoint create \
  --resource-group $RESOURCE_GROUP \
  --name pe-keyvault \
  --vnet-name $VNET_NAME \
  --subnet $DATABRICKS_PRIVATE_SUBNET \
  --private-connection-resource-id $(az keyvault show --name $KEY_VAULT_NAME --resource-group $RESOURCE_GROUP --query id -o tsv) \
  --group-id vault \
  --connection-name keyvault-connection
```

---

## 4. Security Configuration

### 4.1 Managed Identities

```bash
# Create user-assigned managed identity
MANAGED_IDENTITY="mi-databricks-platform"

az identity create \
  --resource-group $RESOURCE_GROUP \
  --name $MANAGED_IDENTITY

# Assign Storage Blob Data Contributor role
az role assignment create \
  --assignee $(az identity show --name $MANAGED_IDENTITY --resource-group $RESOURCE_GROUP --query principalId -o tsv) \
  --role "Storage Blob Data Contributor" \
  --scope $(az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query id -o tsv)
```

### 4.2 Key Vault Secrets

```bash
# Store Event Hubs connection string
EVENTHUB_CONN_STRING=$(az eventhubs namespace authorization-rule keys list \
  --resource-group $RESOURCE_GROUP \
  --namespace-name $EVENTHUB_NAMESPACE \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name eventhub-connection-string \
  --value "$EVENTHUB_CONN_STRING"

# Store storage account key
STORAGE_KEY=$(az storage account keys list \
  --resource-group $RESOURCE_GROUP \
  --account-name $STORAGE_ACCOUNT \
  --query [0].value -o tsv)

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name storage-account-key \
  --value "$STORAGE_KEY"
```

### 4.3 Databricks Secret Scope

```bash
# Using Databricks CLI
databricks secrets create-scope --scope key-vault-scope --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/<sub-id>/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.KeyVault/vaults/$KEY_VAULT_NAME" \
  --dns-name "https://${KEY_VAULT_NAME}.vault.azure.net/"
```

---

## 5. Unity Catalog Permissions

### 5.1 Create Groups

```sql
-- Create user groups
CREATE GROUP IF NOT EXISTS data_engineers;
CREATE GROUP IF NOT EXISTS data_analysts;
CREATE GROUP IF NOT EXISTS data_admins;
```

### 5.2 Grant Catalog Permissions

```sql
-- Data Engineers - Full access to dev, read on test/prod
GRANT USE CATALOG ON CATALOG dev_data_platform TO data_engineers;
GRANT USE SCHEMA ON CATALOG dev_data_platform TO data_engineers;
GRANT CREATE SCHEMA ON CATALOG dev_data_platform TO data_engineers;
GRANT SELECT, MODIFY ON CATALOG dev_data_platform TO data_engineers;

GRANT USE CATALOG ON CATALOG prod_data_platform TO data_engineers;
GRANT SELECT ON CATALOG prod_data_platform TO data_engineers;

-- Data Analysts - Read access to gold layer only
GRANT USE CATALOG ON CATALOG prod_data_platform TO data_analysts;
GRANT USE SCHEMA ON SCHEMA prod_data_platform.gold_analytics TO data_analysts;
GRANT SELECT ON SCHEMA prod_data_platform.gold_analytics TO data_analysts;

-- Data Admins - Full access everywhere
GRANT ALL PRIVILEGES ON CATALOG dev_data_platform TO data_admins;
GRANT ALL PRIVILEGES ON CATALOG test_data_platform TO data_admins;
GRANT ALL PRIVILEGES ON CATALOG prod_data_platform TO data_admins;
```

---

## 6. Development Environment Setup

### 6.1 Install Databricks CLI

```bash
# Install via pip
pip install databricks-cli

# Configure authentication
databricks configure --token

# Enter:
# - Databricks Host: https://adb-<workspace-id>.<random>.azuredatabricks.net
# - Token: <personal-access-token>
```

### 6.2 Git Integration

```bash
# In Databricks workspace, configure Git integration
# Repos → Add Repo → Connect to Azure DevOps/GitHub

# Clone repo structure:
# /Repos/<username>/data-platform-pipelines/
#   ├── pipelines/
#   │   ├── bronze_kafka_ingestion.py
#   │   ├── bronze_api_ingestion.py
#   │   ├── bronze_sftp_ingestion.py
#   │   ├── silver_cleansing.py
#   │   └── gold_aggregations.py
#   ├── config/
#   │   ├── dev_config.json
#   │   ├── test_config.json
#   │   └── prod_config.json
#   └── tests/
```

### 6.3 Cluster Configuration

**Interactive Development Cluster:**
```json
{
  "cluster_name": "dev-interactive",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  },
  "autotermination_minutes": 30,
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": "true"
  },
  "data_security_mode": "USER_ISOLATION"
}
```

---

## 7. Azure Data Factory Configuration

### 7.1 Create Linked Services

**ADLS Gen2 Linked Service:**
```json
{
  "name": "AzureDataLakeStorage",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "AzureBlobFS",
    "typeProperties": {
      "url": "https://stdataplatformprod.dfs.core.windows.net",
      "tenant": "<tenant-id>",
      "servicePrincipalId": "<sp-id>",
      "servicePrincipalKey": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault",
          "type": "LinkedServiceReference"
        },
        "secretName": "service-principal-key"
      }
    }
  }
}
```

**REST API Linked Service:**
```json
{
  "name": "CustomerAPILinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "RestService",
    "typeProperties": {
      "url": "https://api.customer-service.com/v1",
      "enableServerCertificateValidation": true,
      "authenticationType": "OAuth2ClientCredential",
      "clientId": "<client-id>",
      "clientSecret": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "AzureKeyVault",
          "type": "LinkedServiceReference"
        },
        "secretName": "api-client-secret"
      },
      "tokenEndpoint": "https://auth.customer-service.com/oauth/token"
    }
  }
}
```

### 7.2 Create API Ingestion Pipeline

```json
{
  "name": "pipeline-api-to-landing",
  "properties": {
    "activities": [
      {
        "name": "CallCustomerAPI",
        "type": "Copy",
        "inputs": [
          {
            "referenceName": "CustomerAPIDataset",
            "type": "DatasetReference"
          }
        ],
        "outputs": [
          {
            "referenceName": "ADLSLandingDataset",
            "type": "DatasetReference",
            "parameters": {
              "fileName": {
                "value": "@concat('customer-api-', formatDateTime(utcnow(), 'yyyy-MM-dd-HH-mm-ss'), '.json')",
                "type": "Expression"
              }
            }
          }
        ],
        "typeProperties": {
          "source": {
            "type": "RestSource",
            "httpRequestTimeout": "00:05:00"
          },
          "sink": {
            "type": "JsonSink"
          }
        }
      }
    ]
  }
}
```

---

## 8. Folder Structure in ADLS Gen2

```
data (container)
├── dev/
│   ├── bronze/
│   │   ├── kafka_events/
│   │   ├── api_data/
│   │   └── sftp_files/
│   ├── silver/
│   │   ├── events_clean/
│   │   └── customers/
│   └── gold/
│       └── daily_metrics/
├── test/
│   └── [same structure as dev]
└── prod/
    └── [same structure as dev]

landing-zones (container)
├── api-data/
│   ├── customer-api/
│   └── product-api/
└── sftp-data/
    ├── partner-reconciliation/
    └── legacy-exports/

schemas (container)
└── autoloader/
    ├── customer-api/
    ├── product-api/
    └── partner-recon/
```

---

## 9. Verification Steps

### 9.1 Test Unity Catalog Access

```sql
-- List catalogs
SHOW CATALOGS;

-- List schemas in dev catalog
USE CATALOG dev_data_platform;
SHOW SCHEMAS;

-- Test table creation
CREATE TABLE dev_data_platform.bronze_raw.test_table (
  id INT,
  name STRING
);

-- Clean up
DROP TABLE dev_data_platform.bronze_raw.test_table;
```

### 9.2 Test Event Hubs Connection

```python
# Test notebook
eventhub_connection_string = dbutils.secrets.get(
    scope="key-vault-scope",
    key="eventhub-connection-string"
)

# Try reading from Event Hub
df = (spark.readStream
    .format("eventhubs")
    .options(**{
        "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            eventhub_connection_string
        )
    })
    .load())

display(df.limit(10))
```

### 9.3 Test ADLS Gen2 Access

```python
# List files in landing zone
dbutils.fs.ls("abfss://landing-zones@stdataplatformprod.dfs.core.windows.net/")

# Write test file
test_data = spark.range(10)
test_data.write.format("delta").save(
    "abfss://data@stdataplatformprod.dfs.core.windows.net/dev/bronze/test/"
)
```

---

## 10. Infrastructure as Code (Optional)

### 10.1 Terraform Configuration

```hcl
# main.tf
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "main" {
  name     = "rg-data-platform-prod"
  location = "eastus2"
}

resource "azurerm_storage_account" "main" {
  name                     = "stdataplatformprod"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  is_hns_enabled          = true
}

# ... more resources
```

---

## Next Steps

Infrastructure setup complete! Now implement the Bronze layer:

→ **[03-Bronze-Layer.md](03-Bronze-Layer.md)** - Start building data ingestion pipelines

---

**References:**
- ← [01-Architecture-Overview.md](01-Architecture-Overview.md)
- → [03-Bronze-Layer.md](03-Bronze-Layer.md)
- ↑ [00-Index.md](00-Index.md)
