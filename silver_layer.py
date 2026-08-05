"""
Silver Layer - Data Quality & Transformations
==============================================
Responsibility: Data cleansing, validation, business logic

Design Decisions:
1. @dp.expect_or_drop instead of @dp.expect:
   - expect_or_drop: Rejects invalid rows (better data quality in Gold)
   - expect: Would only warn but let bad data through
   - For production: Better to have less but clean data
   
2. Expectations documented as separate functions:
   - Each rule has a name and constraint
   - Metrics are visible in Unity Catalog Event Log
   
3. Streaming vs. Batch pattern:
   - silver_taxi_trips: Streaming table (@dp.table + dp.readStream)
   - silver_taxi_zones: Materialized view (@dp.materialized_view + dp.read)
   - Best Practice: Match decorator to data source type
   
4. Calculated columns (trip_duration_minutes, speed_mph):
   - Business logic centrally defined in Silver
   - Gold layer can directly access them

5. Watermarking for Deduplication:
   - 2-day watermark on pickup_datetime bounds State Store growth
   - Duplicates only realistic at month-boundary file overlaps (hours, not months)
   - Trade-off: late data beyond 2 days won't be deduplicated (acceptable risk)
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark import pipelines as dp

# Import reusable transformation functions (testable business logic)
from transformations import (
    standardize_trip_schema,
    calculate_trip_metrics
)


@dp.table(
    comment="Silver layer: Validated and cleaned NYC taxi trips with quality checks"
)
@dp.expect_or_drop("valid_trip_distance", "trip_distance > 0 AND trip_distance < 1000")
@dp.expect_or_drop("valid_fare", "fare_amount > 0 AND fare_amount < 10000")
@dp.expect_or_drop("valid_passenger_count", "passenger_count > 0 AND passenger_count <= 8")
@dp.expect_or_drop("valid_timestamps", "pickup_datetime < dropoff_datetime")
@dp.expect_or_drop("no_nulls_in_key_fields", "pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL")
def silver_taxi_trips() -> DataFrame:
    """
    Cleaned and validated trip data with business logic.
    
    Quality Rules:
    - Distance: 0 < trip_distance < 1000 miles
    - Fare: 0 < fare_amount < $10,000
    - Passengers: 1-8 people
    - Time logic: pickup before dropoff
    - No nulls in key timestamp fields

    
    Returns:
        DataFrame with clean trips + calculated columns
    """
    bronze_df = spark.readStream.table("bronze_taxi_trips")  # Streaming! Maintains streaming chain from Bronze
    
    return (
        bronze_df
        # Apply reusable transformations (testable business logic)
        .transform(standardize_trip_schema)   # Normalize column names from raw schema
        .transform(calculate_trip_metrics)    # Calculate duration, speed, date/time fields
        
        # Select final schema (standardized for downstream)
        .select(
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "trip_date",
            "pickup_hour",
            "pickup_day_of_week",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "pickup_location_id",
            "dropoff_location_id",
            "trip_duration_minutes",
            "speed_mph",
            "_ingestion_timestamp",
            "_source_file"
        )
        
        # Watermark: Limit State Store growth for deduplication
        # Allows late data up to 2 days - balances Auto Loader discovery delays
        # with realistic duplicate window (month-boundary overlaps)
        .withWatermark("pickup_datetime", "2 days")
        
        # Deduplication based on pickup time, vendor and location
        # (Databricks dataset may have duplicates across files)
        # Watermark ensures State Store doesn't grow unbounded in production
        .dropDuplicates(["pickup_datetime", "vendor_id", "pickup_location_id"])
    )


@dp.materialized_view(
    comment="Silver layer: NYC taxi zone dimension. Materialized view for batch/static reference data."
)
@dp.expect_or_drop("valid_location_id", "location_id IS NOT NULL")
@dp.expect_or_drop("valid_borough", "borough IS NOT NULL AND borough != ''")
def silver_taxi_zones() -> DataFrame:
    """
    Dimension table for taxi zones (batch/static reference data).
    
    Design Notes:
    - Materialized View (not streaming) - zones are relatively static
    - Gold layer uses AUTO CDC FROM SNAPSHOT for SCD Type 2 history
    - No Change Data Feed needed - snapshot-based CDC works for batch sources
    - Bronze keeps raw column names (LocationID, Borough, Zone)
    - Silver standardizes to lowercase snake_case (location_id, borough, zone)
    
    Data Source: bronze_taxi_zones (from Databricks taxizone dataset)
    
    Returns:
        DataFrame with zone dimensions (standardized column names)
    """
    return (
        spark.read.table("bronze_taxi_zones")
        # Standardize column names (Bronze has raw names from source)
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .select(
            "location_id",
            "borough",
            "zone",
            F.col("service_zone"),  # Already lowercase in Bronze
            "_ingestion_timestamp"
        )
    )
