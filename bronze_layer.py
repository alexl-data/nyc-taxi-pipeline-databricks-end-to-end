"""
Bronze Layer - NYC Taxi Data Ingestion
========================================
Responsibility: Raw data ingestion from Databricks sample dataset using Auto Loader.
Bronze = Raw data as-is, minimal transformations (only audit columns + basic quality checks).

Design Decisions:
1. Auto Loader (cloudFiles) instead of spark.read: 
   - Incremental processing: Only new files are loaded
   - Schema evolution: New columns are automatically added
   - Checkpointing: Guarantees exactly-once semantics
   
2. Schema inference with cloudFiles.inferColumnTypes:
   - Automatic type detection (string, int, double, timestamp)
   - Reduces manual schema maintenance effort
   
3. Audit columns (_ingestion_timestamp, _source_file):
   - Traceability: Which file, when loaded
   - Debugging: Identify source on data issues
   - Data lineage: Complete audit trail from source to output

4. Data Source - Databricks Sample Dataset:
   - Using dbfs:/databricks-datasets/nyctaxi/ (readily available)
   - For production: Migrate to Unity Catalog Volumes (see comments below)

5. Environment Separation:
   - Schema locations parametrized via spark.conf to avoid dev/prod conflicts
   - Set via DABS configuration per target environment

6. Data Quality Expectations:
   - Only technical / structural checks (NULL in core fields = broken record)
   - Bronze = Raw as-is, reject only technically incomplete/unparseable records

7. Monitoring & Observability:
   - Event Log published to <catalog>.<schema>.event_log (configured in databricks.yml)
   - Query example: SELECT * FROM <catalog>.<schema>.event_log WHERE event_type = 'flow_progress'
   - Recommended: Set up Databricks SQL Alerts for high drop rates (>10%) - not yet implemented

8. Full Refresh Strategy:
   - Set full_refresh: true in databricks.yml job task (line ~193) to reprocess all data
   - Deletes Auto Loader checkpoints and pipeline state, reprocesses from scratch
   - Use cases: Schema changes, bug fixes in transformation logic, pipeline architecture changes
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark import pipelines as dp


@dp.table(
    comment="Bronze layer: Raw NYC taxi trip data ingested from Databricks sample dataset using Auto Loader"
)
@dp.expect_or_drop("trip_distance_not_null", "trip_distance IS NOT NULL")
@dp.expect_or_drop("pickup_datetime_not_null", "tpep_pickup_datetime IS NOT NULL")
@dp.expect_or_drop("dropoff_datetime_not_null", "tpep_dropoff_datetime IS NOT NULL")
@dp.expect_or_drop("fare_amount_not_null", "fare_amount IS NOT NULL")
def bronze_taxi_trips() -> DataFrame:
    """
    Loads NYC Taxi trip data incrementally from Databricks sample dataset.
    
    Data Source: dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/
    Format: CSV files with headers
    Schema: Modern Yellow Taxi (2019+) with TLC standard schema
            (tpep_pickup_datetime, PULocationID, DOLocationID, fare_amount, etc.)
    
    Data Quality (Technical Checks Only):
    - Drops records with NULL in core fields (technical data quality)
    - NO business logic checks (e.g., distance > 0, fare > 0) - those belong in Silver
    
    For Production Migration to UC Volumes:
    1. Create volume in target environment:
       CREATE VOLUME nyc_taxi.<schema>.raw_data;
       -- Dev:  CREATE VOLUME nyc_taxi.nyc_taxi_dev.raw_data;
       -- Prod: CREATE VOLUME nyc_taxi.nyc_taxi_prod.raw_data;
       
    2. Copy data to volume:
       dbutils.fs.cp("dbfs:/databricks-datasets/nyctaxi/", 
                     "/Volumes/nyc_taxi/<schema>/raw_data/nyctaxi/", 
                     recurse=True)
                     
    3. Update .load() path:
       .load("/Volumes/nyc_taxi/<schema>/raw_data/nyctaxi/tripdata/yellow/")
       -- Replace <schema> with nyc_taxi_dev or nyc_taxi_prod depending on target
       
    4. Update databricks.yml to set schema_location per target:
       targets:
         dev:
           variables:
             bronze_schema_location: "/tmp/dev/bronze_taxi_trips_schema"
         prod:
           variables:
             bronze_schema_location: "/tmp/prod/bronze_taxi_trips_schema"
    
    Returns:
        DataFrame with raw data + audit columns
    """
    # Environment-specific schema location (parametrized via DABS)
    schema_location = spark.conf.get(
        "bronze.schema_location",
        "/tmp/bronze_taxi_trips_schema"  # Fallback for local development
    )
    
    # Auto Loader configuration for incremental processing
    # NOTE: Load only 2019+ files to ensure consistent modern schema
    #       For production: copy only yellow_tripdata_2019*.csv.gz and later to your volume
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")  # Source format for Databricks dataset
        .option("header", "true")  # CSV has headers
        .option("cloudFiles.inferColumnTypes", "true")  # Automatic type inference
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Schema evolution
        .option("cloudFiles.schemaLocation", schema_location)  # Environment-specific checkpoint
        .load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019*.csv.gz")  # Only 2019+ for consistent schema
    )
    
    # Note: 2019+ files have clean column names (verified via manual inspection)
    # No whitespace trimming or de-duplication needed
    
    # Add audit columns
    return df.withColumns({
        "_ingestion_timestamp": F.current_timestamp(),  # Audit: When loaded
        "_source_file": F.col("_metadata.file_path")  # Audit: From which file (modern approach)
    })


@dp.materialized_view(
    comment="Bronze layer: NYC taxi zone lookup data (dimension). Materialized view for batch/static reference data. RAW data as-is."
)
@dp.expect_or_drop("location_id_not_null", "LocationID IS NOT NULL")
@dp.expect_or_drop("zone_not_null", "Zone IS NOT NULL")
def bronze_taxi_zones() -> DataFrame:
    """
    Loads taxi zone lookup table (relatively static dimension data).
    
    Data Source: dbfs:/databricks-datasets/nyctaxi/taxizone/
    Format: CSV with headers
    
    Design: 
    - Materialized view (not streaming table) because zones rarely change
    
    Data Quality (Technical Checks Only):
    - Drops records with NULL in core fields (LocationID, Zone)
    - NO business logic checks - only structural completeness
    
    Schema (raw):
    - LocationID: Zone identifier (raw name from source)
    - Borough: NYC borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island)
    - Zone: Specific zone name
    - service_zone: Service zone classification
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_zone_lookup.csv")
        .withColumns({
            "_ingestion_timestamp": F.current_timestamp(),
            "_source_file": F.col("_metadata.file_path")  # Modern approach for metadata
        })
    )
