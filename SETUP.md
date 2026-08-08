# NYC Taxi Pipeline - Setup Guide

This guide covers **CI/CD setup**, **troubleshooting**, and **local testing** – everything not already in the [README](README.md).

> **Note:** For architecture, design decisions, and quick start deployment, see the [README](README.md).

---

## 🔐 CI/CD Setup (GitHub Actions)

### 1. Configure GitHub Secrets

Go to: **GitHub Repo → Settings → Secrets → Actions**

**For Production Setup (Recommended):**

Create separate service principals for dev and prod environments:

```bash
# In Databricks Workspace: Settings → Identity and Access → Service Principals

# 1. Create service principal for dev
# 2. Create service principal for prod (separate!)
# 3. Generate tokens for both
# 4. Assign permissions:
#    - Dev SP: CAN_MANAGE on nyc_taxi.nyc_taxi_dev schema
#    - Prod SP: CAN_MANAGE on nyc_taxi.nyc_taxi_prod schema
```

Then add these GitHub secrets:

```
DATABRICKS_HOST=https://dbc-XXXXX.cloud.databricks.com
DATABRICKS_TOKEN_DEV=<dev_service_principal_token>
DATABRICKS_TOKEN_PROD=<prod_service_principal_token>
```

**Why separate service principals?**
* **Least privilege**: Dev SP can't touch prod data
* **Audit trail**: Know which environment was accessed
* **Incident isolation**: Compromised dev token doesn't affect prod

### 2. Configure GitHub Environments (Optional)

For production approvals:

```
GitHub Repo → Settings → Environments

1. Create "production" environment
2. Required reviewers: <Your name or team>
3. Environment protection rules:
   - Required reviewers: 1+
   - Wait timer: 0 minutes
```

---

## 🧪 Local Testing Setup

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Install dependencies
pip install pytest pyspark pandas ruff black

# 3. Run tests
pytest test_transformations.py -v

# 4. Linting
ruff check *.py

# 5. Formatting
black --check *.py
```

**What the tests cover:**
* Schema standardization logic (`standardize_trip_schema()`)
* Business logic (trip duration, speed calculations)
* Data quality expectations (null checks, range validation)

---

## 🛠️ Troubleshooting

### Problem: Pipeline Fails with "Catalog not found"

**Cause:** Unity Catalog `nyc_taxi` doesn't exist.

**Fix:**
```sql
-- Create catalog and schemas
CREATE CATALOG IF NOT EXISTS nyc_taxi
  COMMENT 'NYC Taxi pipeline catalog';

USE CATALOG nyc_taxi;

CREATE SCHEMA IF NOT EXISTS nyc_taxi_dev
  COMMENT 'Development environment';

CREATE SCHEMA IF NOT EXISTS nyc_taxi_prod
  COMMENT 'Production environment';

-- Verify
SHOW CATALOGS;
SHOW SCHEMAS IN nyc_taxi;
```

---

### Problem: Auto Loader Can't Read Files

**Cause:** Databricks sample dataset not accessible or wrong path.

**Fix:**
```python
# Verify dataset exists
display(dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/"))

# Should show files like:
# yellow_tripdata_2019-01.csv.gz
# yellow_tripdata_2019-02.csv.gz
# ...

# If missing, check workspace permissions or contact admin
```

---

### Problem: Schema Inference Errors

**Cause:** NYC taxi CSV files have schema variations across years. The pipeline targets 2019 only, but column names can still vary.

**Symptoms:**
```
AnalysisException: Column 'tpep_pickup_datetime' not found
```

**Fix:**

The `silver_layer.py` already handles this via `coalesce()` for common variants. If you see new errors:

```python
# 1. Check actual schema of a sample file
sample_df = spark.read.format("csv").option("header", "true").load(
    "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz"
)
sample_df.printSchema()

# 2. Add coalesce for missing columns in silver_layer.py
# Example:
"pickup_datetime": F.coalesce(
    F.col("tpep_pickup_datetime"),
    F.col("pickup_datetime"),  # Add new variant here
).cast("timestamp"),
```

---

### Problem: Expectations Fail (Data Quality)

**Cause:** Invalid data in source (negative fares, impossible distances, etc.).

**Symptoms:**
```
Expectation 'valid_fare' dropped 1234 rows
```

**Diagnosis:**
```sql
-- Check raw data for invalid values
SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN trip_distance <= 0 OR trip_distance >= 1000 THEN 1 END) as invalid_distance,
    COUNT(CASE WHEN fare_amount <= 0 OR fare_amount >= 10000 THEN 1 END) as invalid_fare,
    COUNT(CASE WHEN passenger_count <= 0 OR passenger_count > 8 THEN 1 END) as invalid_passengers
FROM nyc_taxi.nyc_taxi_dev.bronze_taxi_trips;
```

**Fix:**

1. **Option A:** Adjust expectation thresholds in `silver_layer.py`
2. **Option B:** Investigate source data quality (are these real outliers or data entry errors?)
3. **Option C:** Use `@dp.expect_or_fail` if you want pipeline to fail instead of drop


---

### Problem: CI/CD Pipeline Fails with "Insufficient Permissions"

**Cause:** Service principal doesn't have CAN_MANAGE on schema.

**Fix:**
```sql
-- Grant permissions to service principal
GRANT USE CATALOG ON CATALOG nyc_taxi TO <service_principal_name>;
GRANT ALL PRIVILEGES ON SCHEMA nyc_taxi.nyc_taxi_dev TO <service_principal_name>;
```

---

## 📊 Post-Deployment Validation

After successful deployment, run these quick checks:

```sql
-- 1. Verify all tables exist
SHOW TABLES IN nyc_taxi.nyc_taxi_dev;

-- Expected:
-- bronze_taxi_trips, bronze_taxi_zones
-- silver_taxi_trips, silver_taxi_zones
-- fact_taxi_trips
-- dim_date, dim_vendor, dim_taxi_zones_scd
-- gold_daily_revenue_by_zone
-- event_log

-- 2. Test star schema join (verifies relationships work)
SELECT 
    d.day_name,
    v.vendor_name,
    COUNT(*) as trip_count
FROM nyc_taxi.nyc_taxi_dev.fact_taxi_trips f
JOIN nyc_taxi.nyc_taxi_dev.dim_date d ON f.trip_date = d.date
JOIN nyc_taxi.nyc_taxi_dev.dim_vendor v ON f.vendor_id = v.vendor_id
WHERE d.year = 2019
GROUP BY d.day_name, v.vendor_name
LIMIT 10;
```

> **Note:** For detailed monitoring queries (row counts, data quality metrics, lineage), see the [README](README.md) "Monitoring & Validation" section.

---