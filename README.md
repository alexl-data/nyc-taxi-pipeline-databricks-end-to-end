# NYC Taxi Medallion Pipeline

A production-ready data engineering pipeline demonstrating the Medallion Architecture on Databricks.

## Overview

This project implements a complete **Bronze → Silver → Gold** pipeline for NYC Taxi trip data using modern Databricks technologies:

* **Auto Loader** for incremental data ingestion
* **Data Quality Expectations** with automatic tracking
* **AUTO CDC** for declarative SCD Type 2 (no manual MERGE needed)
* **Databricks Asset Bundles** for multi-environment deployment
* **CI/CD** via GitHub Actions

The Gold layer implements a **complete Star Schema** (Kimball methodology) with 1 fact table and 3 dimensions for maximum analytical flexibility.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        RAW DATA SOURCES                         │
│  Databricks Sample Dataset: dbfs:/databricks-datasets/nyctaxi/  │
│    - tripdata/yellow/yellow_tripdata_2019*.csv.gz (2019 only)   │
│    - taxizone/taxi_zone_lookup.csv (Zone lookup)                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                       BRONZE LAYER (Ingestion)                  │
│  • Auto Loader: Incremental processing + Schema Evolution       │
│  • Format: CSV with headers → Auto schema inference             │
│  • Audit Columns: _ingestion_timestamp, _source_file            │
│  Tables: nyc_taxi.{schema}.bronze_taxi_*                        │
│    - bronze_taxi_trips (Streaming from CSV)                     │
│    - bronze_taxi_zones (Batch from CSV)                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleansing)                     │
│  • Data Quality: @dp.expect_or_drop                             │
│  • Schema Standardization: Handles column name variations       │
│  • Watermarking for Deduplication                               │
│  • Business Logic: Calculated columns (duration, speed)         │
│  Tables: nyc_taxi.{schema}.silver_taxi_*                        │
│    - silver_taxi_trips                                          │
│    - silver_taxi_zones                                          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Star Schema)                     │
│                                                                 │
│  Fact Table:                                                    │
│    • fact_taxi_trips (10M+ rows, one row per trip)              │
│                                                                 │
│  Dimensions:                                                    │
│    • dim_date           → Time dimension (2019-2024)            │
│    • dim_vendor         → Service providers (CMT, VTS)          │
│    • dim_taxi_zones_scd → Locations with SCD Type 2 history     │
│                           (via AUTO CDC Flow)                   │
│                                                                 │
│  Relationships:                                                 │
│    fact_taxi_trips.trip_date ───────────► dim_date.date         │
│    fact_taxi_trips.vendor_id ───────────► dim_vendor.vendor_id  │
│    fact_taxi_trips.pickup_location_id ──► dim_taxi_zones_scd    │
│    fact_taxi_trips.dropoff_location_id ─► dim_taxi_zones_scd    │
│                                                                 │
│  Optional:                                                      │
│    • gold_daily_revenue_by_zone (pre-aggregated for dashboards) │
└─────────────────────────────────────────────────────────────────┘
```

**Environments:**
* Dev: `nyc_taxi.nyc_taxi_dev.*`
* Prod: `nyc_taxi.nyc_taxi_prod.*`

---

## 💡 Key Design Decisions

### 1. Auto Loader for Incremental Processing

Even though we process only 2019 data (12 monthly files), Auto Loader provides production benefits over batch reads:

* **Incremental processing**: Only new monthly files are loaded (simulates production monthly drops)
* **Exactly-once semantics**: Checkpointing prevents duplicate processing if pipeline restarts
* **Resilience**: Corrupted CSV rows don't fail the entire pipeline
* **Future-proof**: If we expand to 2020+ data later, no code changes needed

**Why not batch?** `spark.read.csv("path/*")` re-reads all files on every run, has no checkpoint state, and fails on any single corrupted row.

Auto Loader is the Databricks-recommended pattern for any file-based ingestion, even single-year datasets.

---

### 2. Data Quality with @dp.expect_or_drop

Drops invalid rows instead of letting them through. Metrics tracked in Unity Catalog Event Log.

```python
@dp.expect_or_drop("valid_trip_distance", "trip_distance > 0 AND trip_distance < 1000")
@dp.expect_or_drop("valid_fare", "fare_amount > 0 AND fare_amount < 10000")
@dp.expect_or_drop("valid_passenger_count", "passenger_count > 0 AND passenger_count <= 8")
@dp.expect_or_drop("valid_timestamps", "pickup_datetime < dropoff_datetime")
@dp.expect_or_drop("no_nulls_in_key_fields", "pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL")
```

---

### 3. Schema Standardization & Data Scope

**Design Decision:** Pipeline processes only 2019 data (`yellow_tripdata_2019*.csv.gz`).

**Why not multi-year?** NYC taxi schemas are incompatible across years:
* Pre-2016: Uses lat/long coordinates (no location_id)
* 2016-2018: Uses location_id but different column names (`total_amount` ≠ `fare_amount`)
* 2019+: Stable schema with `tpep_pickup_datetime`, `location_id`, standardized amounts

**Merging these would require lossy transformations** (geocode lat/long → zones, or drop pre-2016 data). Instead, we use a single, clean 2019 schema for production-grade data quality.

```python
# transformations.py - Standardizes 2019 column names only
def standardize_trip_schema(df: DataFrame) -> DataFrame:
    return df.withColumns({
        "pickup_datetime": F.col("tpep_pickup_datetime").cast("timestamp"),
        "dropoff_datetime": F.col("tpep_dropoff_datetime").cast("timestamp"),
        # ... rest of 2019 schema
    })
```

---

### 4. Complete Star Schema (Kimball Methodology)

Implements a dimensional model with **1 fact table + 3 dimensions** for maximum analytical flexibility.

**Structure:**
* `fact_taxi_trips` - Detail-level facts (one row per trip)
* `dim_date` - Time dimension with calendar attributes, weekends, holidays
* `dim_vendor` - Service provider dimension
* `dim_taxi_zones_scd` - Location dimension with SCD Type 2 history

Enables any ad-hoc aggregation: revenue by day-of-week, vendor comparison, borough analysis, holiday patterns, etc.

---

### 5. SCD Type 2 via AUTO CDC

Declarative approach using Change Data Feed. No manual MERGE SQL needed.

```python
# Step 1: Create target streaming table for AUTO CDC
dp.create_streaming_table(
    name="dim_taxi_zones_scd",
    comment="Gold layer: Taxi zones dimension with SCD Type 2 history tracking via AUTO CDC FROM SNAPSHOT."
)

# Step 2: AUTO CDC Flow creates SCD Type 2 dimension automatically
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_taxi_zones_scd",
    source="silver_taxi_zones",
    keys=["location_id"],
    stored_as_scd_type=2  # Creates __START_AT, __END_AT for history tracking
)
```

**Why AUTO CDC over manual MERGE?**
* **Declarative**: 2 function calls vs. 50+ lines of MERGE SQL
* **Robust**: Handles late-arriving data and duplicates automatically
* **Maintainable**: No complex SQL logic to debug
* **Databricks-native**: Leverages pipeline optimization

---

### 6. Databricks Asset Bundles for Deployment

Infrastructure as code with separate dev/prod environments and atomic deployments.

```yaml
targets:
  dev:
    schema: nyc_taxi_dev
  prod:
    schema: nyc_taxi_prod
```

---

### 7. Lakeflow Jobs for Orchestration

Scheduled runs with email alerting and automatic retries. Separation of concerns: Pipeline handles data processing logic, Job handles orchestration.

**Alternative:** External orchestration (Airflow, Prefect) would require additional infrastructure.

---

### 8. Optional Pre-Aggregated Tables

`gold_daily_revenue_by_zone` demonstrates performance optimization pattern: pre-aggregate known queries for dashboard speed while keeping detail facts for flexibility.

---

## 📁 Project Structure

```
nyc_taxi_pipeline/
├── databricks.yml              # DABs configuration (Pipeline + Jobs)
├── transformations.py          # Core business logic (testable functions)
├── test_transformations.py     # Unit tests (pytest)
├── bronze_layer.py             # Auto Loader ingestion
├── silver_layer.py             # Data quality + schema standardization
├── gold_layer.py               # Star schema: fact + dimensions
├── github_workflows_ci.yml     # CI: Validation, Linting, Unit Tests
├── github_workflows_cd.yml     # CD: Deployment (dev/prod)
├── README.md                   # This document
└── SETUP.md                    # Detailed setup guide
```

**Note:** CI/CD workflow files are provided as templates. To use them in GitHub Actions, copy them to `.github/workflows/` in your repository.

---

## 🚀 Quick Start

### Prerequisites
* Databricks Workspace (AWS/Azure/GCP) with Unity Catalog enabled
* Databricks CLI installed ([installation guide](https://docs.databricks.com/en/dev-tools/cli/install.html))

### Unity Catalog Setup

Create a dedicated catalog for this project:

```sql
-- Run in Databricks SQL Editor or notebook
CREATE CATALOG IF NOT EXISTS nyc_taxi
  COMMENT 'NYC Taxi pipeline catalog';

-- Verify catalog exists
SHOW CATALOGS;
```

### Setup

```bash
# 1. Clone repository
git clone https://github.com/alexl-data/nyc-taxi-pipeline-databricks-end-to-end.git
cd nyc_taxi_pipeline_databricks_end_to_end

# 2. Configure Databricks CLI
databricks configure --token
# Enter your workspace URL and personal access token when prompted

# 3. Deploy to dev
databricks bundle validate --target dev
databricks bundle deploy --target dev

# 4. Run pipeline in Databricks UI
#    Navigate to the deployed pipeline (nyc_taxi_pipeline_dev) and click "Start"
```

See [SETUP.md](SETUP.md) for detailed instructions including CI/CD setup and troubleshooting.

---

## 🚀 CI/CD Workflows

### CI (Continuous Integration) - `github_workflows_ci.yml`
Runs on every pull request to main:

1. **Bundle Validation:** `databricks bundle validate` checks YAML syntax and resource references
2. **Python Linting:** `ruff` + `black` enforce PEP 8 and code style
3. **Unit Tests:** `pytest` runs all tests in `test_transformations.py`
4. **Coverage Report:** Measures test coverage for `transformations.py` only (not pipeline layers)

### CD (Continuous Deployment) - `github_workflows_cd.yml`
Deployment to dev/prod environments:

* **Dev:** Automatic deployment on push to main
* **Prod:** Manual deployment via `workflow_dispatch` (GitHub UI)
  * For approval gates: Configure GitHub Environment Protection Rules (Settings → Environments → production → Required reviewers)

**Key Design:** CI validates code quality, CD deploys to Databricks. Separation ensures broken code never reaches environments.

---

## 📊 Monitoring & Validation

### Verify Pipeline Ran Successfully
```sql
-- Check all tables exist and have data
SELECT COUNT(*) as trip_count FROM nyc_taxi.nyc_taxi_dev.fact_taxi_trips;
SELECT COUNT(*) as date_count FROM nyc_taxi.nyc_taxi_dev.dim_date;
SELECT COUNT(*) as vendor_count FROM nyc_taxi.nyc_taxi_dev.dim_vendor;
SELECT COUNT(*) as zone_count FROM nyc_taxi.nyc_taxi_dev.dim_taxi_zones_scd WHERE __END_AT IS NULL;
```

### Data Quality Metrics
View expectation results from `@dp.expect_or_drop` (published as Unity Catalog table):

```sql
-- See which records passed/failed data quality checks
SELECT 
  timestamp,
  update_id,
  regexp_extract(expectation_path, 'pipelines\\.expectations\\.(.*)', 1) as table_name,
  metrics.total as total_records,
  metrics.dropped as failed_records,
  metrics.total - metrics.dropped as passed_records
FROM (
  SELECT 
    timestamp,
    origin.update_id as update_id,
    explode(
      from_json(
        from_json(details, 'stream_progress STRUCT<progress_json:STRING>').stream_progress.progress_json,
        'observedMetrics MAP<STRING, STRUCT<total:BIGINT, dropped:BIGINT>>'
      ).observedMetrics
    ) as (expectation_path, metrics)
  FROM nyc_taxi.nyc_taxi_dev.event_log
  WHERE event_type = 'stream_progress' AND details LIKE '%observedMetrics%'
)
ORDER BY timestamp DESC
LIMIT 20;
```

The event log table is created automatically when the pipeline runs (configured via the `event_log:` block in `databricks.yml`).

### Unity Catalog Lineage
```
Data → Catalog Explorer → nyc_taxi → nyc_taxi_dev → <table_name> → Lineage Tab
```

Expected flow: Bronze → Silver → Gold (fact + dims)

---

## 🛠️ Technologies

* **Lakeflow Spark Declarative Pipelines** (Serverless) - dp-API for Bronze/Silver/Gold layers
* **PySpark** - Data transformations and business logic
* **Auto Loader** - Incremental file ingestion (Databricks, built on Spark Structured Streaming)
* **Delta Lake** - ACID transactions, Change Data Feed, Time Travel
* **Unity Catalog** - Centralized governance and metadata
* **Databricks Asset Bundles** - Infrastructure as code
* **GitHub Actions** - CI/CD automation
* **pytest** - Unit testing framework for business logic
