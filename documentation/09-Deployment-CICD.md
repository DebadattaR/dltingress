# 09 - Deployment & CI/CD

**Related Documents:**
- ← [08-Monitoring-Alerting.md](08-Monitoring-Alerting.md) - Monitoring & Alerting
- → [10-Operations-Runbook.md](10-Operations-Runbook.md) - Operations Runbook
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md) - Architecture Overview

---

## 1. CI/CD Strategy

### 1.1 Deployment Pipeline

```
Developer Push to Git
    ↓
CI Build & Test (Azure DevOps / GitHub Actions)
    ↓
Deploy to Dev Environment
    ↓
Run Integration Tests
    ↓
Deploy to Test Environment
    ↓
Run Acceptance Tests
    ↓
Manual Approval
    ↓
Deploy to Production
    ↓
Smoke Tests
```

### 1.2 Environment Promotion

| Environment | Purpose | Deployment | Testing |
|-------------|---------|------------|---------|
| **Dev** | Individual development | On every commit | Unit tests |
| **Test** | Integration testing | On merge to `develop` | Integration + E2E tests |
| **Prod** | Production workloads | On merge to `main` (with approval) | Smoke tests only |

---

## 2. Git Repository Structure

### 2.1 Repository Layout

```
data-platform-pipelines/
├── .github/workflows/          # GitHub Actions (or)
├── .azure-pipelines/          # Azure DevOps pipelines
├── pipelines/
│   ├── bronze_kafka_ingestion.py
│   ├── bronze_api_ingestion.py
│   ├── bronze_sftp_ingestion.py
│   ├── silver_cleansing.py
│   └── gold_aggregations.py
├── config/
│   ├── dev.json
│   ├── test.json
│   └── prod.json
├── tests/
│   ├── unit/
│   │   ├── test_transformations.py
│   │   └── test_data_quality.py
│   └── integration/
│       └── test_end_to_end.py
├── databricks-bundle/
│   ├── databricks.yml          # Databricks Asset Bundle config
│   └── resources/
│       └── pipelines.yml
├── terraform/                  # Infrastructure as Code (optional)
│   ├── main.tf
│   └── variables.tf
├── requirements.txt
└── README.md
```

### 2.2 Branching Strategy

```
main (production)
    ↑
release/v1.0.0 (release candidate)
    ↑
develop (integration)
    ↑
feature/bronze-kafka-updates
feature/silver-quality-improvements
```

**Branch Protection:**
- `main`: Requires approval, all tests pass
- `develop`: Requires PR review, tests pass
- `feature/*`: No restrictions

---

## 3. Databricks Asset Bundles (DABs)

### 3.1 Bundle Configuration

**File:** `databricks-bundle/databricks.yml`

```yaml
bundle:
  name: data-platform-pipelines

# Workspace configuration
workspace:
  host: https://adb-<workspace-id>.azuredatabricks.net
  root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.environment}

# Targets (environments)
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<dev-workspace-id>.azuredatabricks.net
    resources:
      pipelines:
        bronze_ingestion:
          name: "[DEV] Bronze Ingestion"
          catalog: dev_data_platform
          target: bronze_raw
          configuration:
            environment: dev
  
  test:
    mode: development
    workspace:
      host: https://adb-<test-workspace-id>.azuredatabricks.net
    resources:
      pipelines:
        bronze_ingestion:
          name: "[TEST] Bronze Ingestion"
          catalog: test_data_platform
          target: bronze_raw
          configuration:
            environment: test
  
  prod:
    mode: production
    workspace:
      host: https://adb-<prod-workspace-id>.azuredatabricks.net
    resources:
      pipelines:
        bronze_ingestion:
          name: "[PROD] Bronze Ingestion"
          catalog: prod_data_platform
          target: bronze_raw
          configuration:
            environment: prod
            
# Include pipeline definitions
include:
  - resources/*.yml
```

**File:** `databricks-bundle/resources/pipelines.yml`

```yaml
resources:
  pipelines:
    bronze_kafka:
      name: "${bundle.environment}-bronze-kafka-ingestion"
      catalog: "${bundle.environment}_data_platform"
      target: bronze_raw
      libraries:
        - notebook:
            path: ../pipelines/bronze_kafka_ingestion.py
      continuous: true
      clusters:
        - label: default
          autoscale:
            min_workers: 2
            max_workers: 8
            mode: ENHANCED
          node_type_id: Standard_E8s_v3
    
    silver_cleansing:
      name: "${bundle.environment}-silver-cleansing"
      catalog: "${bundle.environment}_data_platform"
      target: silver_cleansed
      libraries:
        - notebook:
            path: ../pipelines/silver_cleansing.py
      continuous: true
      clusters:
        - label: default
          autoscale:
            min_workers: 2
            max_workers: 8
          node_type_id: Standard_E8s_v3
    
    gold_aggregations:
      name: "${bundle.environment}-gold-aggregations"
      catalog: "${bundle.environment}_data_platform"
      target: gold_analytics
      libraries:
        - notebook:
            path: ../pipelines/gold_aggregations.py
      continuous: false  # Triggered
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # 2 AM daily
        timezone_id: "America/New_York"
      clusters:
        - label: default
          num_workers: 4
          node_type_id: Standard_D8s_v3
```

### 3.2 Deploy with DABs

```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate
databricks configure --token

# Validate bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Deploy to test
databricks bundle deploy --target test

# Deploy to prod
databricks bundle deploy --target prod
```

---

## 4. GitHub Actions CI/CD

### 4.1 CI Pipeline

**File:** `.github/workflows/ci.yml`

```yaml
name: CI - Test and Validate

on:
  pull_request:
    branches: [develop, main]
  push:
    branches: [develop]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      
      - name: Run unit tests
        run: |
          pytest tests/unit/ --cov=pipelines --cov-report=xml
      
      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.xml
  
  validate:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Validate Databricks Bundle
        run: databricks bundle validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

### 4.2 CD Pipeline - Dev

**File:** `.github/workflows/cd-dev.yml`

```yaml
name: CD - Deploy to Dev

on:
  push:
    branches: [develop]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Deploy to Dev
        run: databricks bundle deploy --target dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_DEV_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_DEV_TOKEN }}
      
      - name: Run Integration Tests
        run: |
          python tests/integration/test_end_to_end.py
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_DEV_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_DEV_TOKEN }}
```

### 4.3 CD Pipeline - Production

**File:** `.github/workflows/cd-prod.yml`

```yaml
name: CD - Deploy to Production

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production  # Requires manual approval
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Deploy to Production
        run: databricks bundle deploy --target prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
      
      - name: Run Smoke Tests
        run: |
          python tests/smoke/test_prod_health.py
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
      
      - name: Notify Team
        uses: slackapi/slack-github-action@v1
        with:
          payload: |
            {
              "text": "Production deployment complete for data-platform-pipelines"
            }
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK }}
```

---

## 5. Azure DevOps Pipelines

### 5.1 CI Pipeline

**File:** `.azure-pipelines/ci-pipeline.yml`

```yaml
trigger:
  branches:
    include:
      - develop
      - main

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: Test
    jobs:
      - job: UnitTests
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.10'
          
          - script: |
              pip install -r requirements.txt
              pip install pytest pytest-cov
            displayName: 'Install dependencies'
          
          - script: |
              pytest tests/unit/ --cov=pipelines --cov-report=xml --junitxml=junit.xml
            displayName: 'Run unit tests'
          
          - task: PublishTestResults@2
            inputs:
              testResultsFormat: 'JUnit'
              testResultsFiles: 'junit.xml'
          
          - task: PublishCodeCoverageResults@1
            inputs:
              codeCoverageTool: 'Cobertura'
              summaryFileLocation: 'coverage.xml'
      
      - job: ValidateBundle
        steps:
          - script: |
              pip install databricks-cli
              databricks bundle validate
            displayName: 'Validate Databricks bundle'
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
```

### 5.2 CD Pipeline

**File:** `.azure-pipelines/cd-pipeline.yml`

```yaml
trigger: none  # Manual or via release pipeline

parameters:
  - name: environment
    type: string
    default: 'dev'
    values:
      - dev
      - test
      - prod

pool:
  vmImage: 'ubuntu-latest'

stages:
  - stage: Deploy
    jobs:
      - deployment: DeployPipelines
        environment: ${{ parameters.environment }}
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self
                
                - script: |
                    pip install databricks-cli
                  displayName: 'Install Databricks CLI'
                
                - script: |
                    databricks bundle deploy --target ${{ parameters.environment }}
                  displayName: 'Deploy Databricks bundle'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
                
                - script: |
                    python tests/smoke/test_health.py
                  displayName: 'Run smoke tests'
                  env:
                    DATABRICKS_HOST: $(DATABRICKS_HOST)
                    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)
```

---

## 6. Testing Strategy

### 6.1 Unit Tests

**File:** `tests/unit/test_transformations.py`

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

def test_event_cleansing(spark):
    """Test event cleansing logic"""
    
    # Sample input data
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", StringType(), True)
    ])
    
    input_data = [
        ("E001", "purchase", "100.50"),
        ("E002", "REFUND", "50.25"),
        (None, "purchase", "75.00")  # Invalid - no event_id
    ]
    
    df = spark.createDataFrame(input_data, schema)
    
    # Apply transformation logic (simplified)
    result_df = (
        df
        .filter("event_id IS NOT NULL")
        .withColumn("event_type", upper(col("event_type")))
        .withColumn("amount", col("amount").cast("decimal(18,2)"))
    )
    
    # Assertions
    assert result_df.count() == 2  # One record dropped
    assert result_df.filter("event_type = 'PURCHASE'").count() == 1
    assert result_df.filter("event_type = 'REFUND'").count() == 1
```

### 6.2 Integration Tests

**File:** `tests/integration/test_end_to_end.py`

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState
import time

def test_bronze_to_silver_flow():
    """Test end-to-end data flow from Bronze to Silver"""
    
    w = WorkspaceClient()
    
    # Trigger Bronze pipeline
    pipeline_id = "<bronze-pipeline-id>"
    update = w.pipelines.start_update(pipeline_id=pipeline_id)
    
    # Wait for completion
    while True:
        status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
        if status.state in [PipelineState.COMPLETED, PipelineState.FAILED]:
            break
        time.sleep(30)
    
    assert status.state == PipelineState.COMPLETED, "Pipeline failed"
    
    # Verify data in Silver layer
    spark = SparkSession.builder.getOrCreate()
    silver_df = spark.table("dev_data_platform.silver_cleansed.silver_events_clean")
    
    assert silver_df.count() > 0, "No data in Silver layer"
    assert silver_df.filter("event_id IS NULL").count() == 0, "Invalid data in Silver"
```

### 6.3 Smoke Tests

**File:** `tests/smoke/test_prod_health.py`

```python
from databricks.sdk import WorkspaceClient

def test_production_pipelines_running():
    """Verify all production pipelines are running"""
    
    w = WorkspaceClient()
    
    expected_pipelines = [
        "bronze-kafka-ingestion",
        "silver-cleansing",
        "gold-aggregations"
    ]
    
    for pipeline_name in expected_pipelines:
        pipeline = w.pipelines.get_by_name(pipeline_name)
        assert pipeline.state == "RUNNING", f"{pipeline_name} is not running"

def test_data_freshness():
    """Verify data is fresh (updated within last 2 hours)"""
    
    spark = SparkSession.builder.getOrCreate()
    
    result = spark.sql("""
        SELECT MAX(silver_processed_timestamp) as last_update
        FROM prod_data_platform.silver_cleansed.silver_events_clean
    """).collect()[0]
    
    hours_old = (datetime.now() - result.last_update).total_seconds() / 3600
    
    assert hours_old < 2, f"Data is {hours_old} hours old"
```

---

## 7. Environment Configuration

### 7.1 Configuration Files

**File:** `config/dev.json`

```json
{
  "environment": "dev",
  "catalog": "dev_data_platform",
  "eventhub_namespace": "evhns-data-platform-dev",
  "storage_account": "stdataplatformdev",
  "key_vault": "kv-dataplatform-dev",
  "cluster_config": {
    "min_workers": 2,
    "max_workers": 4,
    "node_type": "Standard_DS3_v2"
  }
}
```

**File:** `config/prod.json`

```json
{
  "environment": "prod",
  "catalog": "prod_data_platform",
  "eventhub_namespace": "evhns-data-platform-prod",
  "storage_account": "stdataplatformprod",
  "key_vault": "kv-dataplatform-prod",
  "cluster_config": {
    "min_workers": 4,
    "max_workers": 16,
    "node_type": "Standard_E8s_v3"
  }
}
```

### 7.2 Load Configuration in Pipelines

```python
# Load environment-specific config
import json

env = spark.conf.get("environment", "dev")
config_path = f"/Workspace/config/{env}.json"

with open(config_path) as f:
    config = json.load(f)

# Use config values
catalog = config["catalog"]
eventhub_namespace = config["eventhub_namespace"]
```

---

## 8. Rollback Strategy

### 8.1 Rollback Pipeline Deployment

```bash
# List recent deployments
databricks bundle deploy --target prod --dry-run

# Rollback to previous version (re-deploy from Git tag)
git checkout v1.0.0
databricks bundle deploy --target prod
```

### 8.2 Rollback Data Changes

```sql
-- Time travel to previous version
CREATE OR REPLACE TABLE prod_data_platform.silver_cleansed.silver_events_clean
SHALLOW CLONE prod_data_platform.silver_cleansed.silver_events_clean
VERSION AS OF 100;

-- Or restore to timestamp
RESTORE TABLE prod_data_platform.silver_cleansed.silver_events_clean
TO TIMESTAMP AS OF '2026-02-08 10:00:00';
```

---

## 9. Best Practices

✅ **Use Databricks Asset Bundles** - Simplifies multi-environment deployments  
✅ **Implement comprehensive testing** - Unit, integration, smoke tests  
✅ **Automate everything** - No manual deployments to production  
✅ **Use environment-specific configs** - Externalize all configuration  
✅ **Require PR reviews** - Code quality and knowledge sharing  
✅ **Tag releases** - Easy rollbacks and version tracking  
✅ **Monitor deployments** - Track success/failure rates  
✅ **Test in production-like environments** - Catch issues early  

---

## Next Steps

CI/CD configured! Review operational procedures:

→ **[10-Operations-Runbook.md](10-Operations-Runbook.md)** - Day-2 operations guide

---

**References:**
- ← [08-Monitoring-Alerting.md](08-Monitoring-Alerting.md)
- → [10-Operations-Runbook.md](10-Operations-Runbook.md)
- ↑ [01-Architecture-Overview.md](01-Architecture-Overview.md)
- [Databricks Asset Bundles](https://learn.microsoft.com/azure/databricks/dev-tools/bundles/)
