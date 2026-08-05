"""
Reusable Transformation Functions for NYC Taxi Pipeline
========================================================
These functions contain the core business logic and are:
1. Unit-testable (no Databricks runtime required)
2. Reusable across Bronze/Silver/Gold layers
3. Decoupled from Lakeflow decorators (@dp.table, @dp.materialized_view)

Design Pattern: Separation of Concerns
---------------------------------------
This demonstrates Software Engineering maturity beyond Data Engineering:
- Business logic (transformations) is separate from orchestration (@dp decorators)
- Pure functions that can be imported and tested in isolation
- Tests validate the ACTUAL production code, not a replica
- Changes to logic automatically flow through to tests
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def calculate_trip_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate derived trip metrics: duration, speed, date/time attributes.
    
    Business Rules:
    - trip_duration_minutes: Time difference in minutes
    - speed_mph: Distance / duration (0 if duration is 0 to avoid division by zero)
    - trip_date: Date of pickup
    - pickup_hour, pickup_day_of_week: Time attributes for analysis
    
    Args:
        df: DataFrame with pickup_datetime, dropoff_datetime, trip_distance
        
    Returns:
        DataFrame with added metric columns
        
    Example:
        >>> bronze_df = spark.readStream.table("bronze_taxi_trips")
        >>> enriched_df = calculate_trip_metrics(bronze_df)
    """
    return (
        df
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60
        )
        .withColumn(
            "speed_mph",
            F.when(F.col("trip_duration_minutes") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_minutes") / 60))
            .otherwise(0)  # Handle zero-duration trips gracefully
        )
        .withColumn("trip_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))
    )


def standardize_trip_schema(df: DataFrame) -> DataFrame:
    """
    Standardize column names from raw Yellow Taxi schema to normalized schema.
    
    Design Decision:
    - Bronze layer keeps raw names (matches source schema exactly)
    - Silver layer normalizes to snake_case for consistency
    - This function bridges the two
    
    Raw Schema (Bronze): tpep_pickup_datetime, PULocationID, VendorID
    Normalized Schema (Silver): pickup_datetime, pickup_location_id, vendor_id
    
    Args:
        df: DataFrame with raw Yellow Taxi column names
        
    Returns:
        DataFrame with standardized column names and types
        
    Example:
        >>> bronze_df = spark.readStream.table("bronze_taxi_trips")
        >>> standardized_df = standardize_trip_schema(bronze_df)
    """
    return (
        df
        .withColumn("pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("pickup_location_id", F.col("PULocationID").cast("int"))
        .withColumn("dropoff_location_id", F.col("DOLocationID").cast("int"))
        .withColumn("vendor_id", F.col("VendorID").cast("string"))
    )


def apply_data_quality_filters(df: DataFrame) -> DataFrame:
    """
    Apply data quality filters matching @dp.expect_or_drop rules in silver_layer.py.
    
    Quality Rules (must match silver_layer.py decorators EXACTLY):
    1. valid_trip_distance: 0 < trip_distance < 1000 miles
    2. valid_fare: 0 < fare_amount < $10,000
    3. valid_passenger_count: 1-8 people
    4. valid_timestamps: pickup before dropoff
    5. no_nulls_in_key_fields: No nulls in key timestamp fields
    
    IMPORTANT: This function replicates the @dp.expect_or_drop decorators
    for testing purposes. If you change the rules in silver_layer.py,
    you MUST update this function to match.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        DataFrame with only valid rows
        
    Example:
        >>> trips_df = spark.read.table("silver_taxi_trips")
        >>> clean_df = apply_data_quality_filters(trips_df)
    """
    return df.filter(
        # 1. valid_trip_distance
        (F.col("trip_distance") > 0) & (F.col("trip_distance") < 1000) &
        # 2. valid_fare
        (F.col("fare_amount") > 0) & (F.col("fare_amount") < 10000) &
        # 3. valid_passenger_count
        (F.col("passenger_count") > 0) & (F.col("passenger_count") <= 8) &
        # 4. valid_timestamps
        (F.col("pickup_datetime") < F.col("dropoff_datetime")) &
        # 5. no_nulls_in_key_fields
        (F.col("pickup_datetime").isNotNull()) & (F.col("dropoff_datetime").isNotNull())
    )


def enrich_with_pickup_zone(trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Enrich trips with pickup zone dimension attributes (borough, zone).
    
    Design Decision:
    - Only enriches PICKUP zone (not dropoff)
    - Matches the actual gold_daily_revenue_by_zone implementation
    - Uses left join to preserve trips even if zone lookup fails
    
    Args:
        trips_df: Fact table with pickup_location_id
        zones_df: Dimension table with location_id, borough, zone
        
    Returns:
        DataFrame with added pickup_borough and pickup_zone columns
        
    Example:
        >>> facts = spark.readStream.table("fact_taxi_trips")
        >>> zones = spark.read.table("dim_taxi_zones_scd").filter(F.col("__END_AT").isNull())
        >>> enriched = enrich_with_pickup_zone(facts, zones)
    """
    return trips_df.join(
        zones_df.select(
            F.col("location_id").alias("pickup_location_id"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone").alias("pickup_zone")
        ),
        on="pickup_location_id",
        how="left"
    )


def aggregate_daily_revenue_by_zone(df: DataFrame) -> DataFrame:
    """
    Aggregate trip data by date and pickup zone (daily revenue by zone).
    
    Business Metrics:
    - total_revenue: Sum of all fares
    - avg_fare: Average fare per trip
    - trip_count: Number of trips
    - total_distance: Sum of all trip distances
    - avg_duration_minutes: Average trip duration
    - total_passengers: Sum of all passengers
    - revenue_per_mile: Total revenue divided by total distance
    
    Design Decision:
    - Pre-aggregated table for performance-critical dashboards
    - Deliberate denormalization (includes dimension attributes)
    - Grain: One row per (date, zone)
    
    Args:
        df: DataFrame with trip_date, pickup_borough, pickup_zone, pickup_location_id,
            fare_amount, trip_distance, trip_duration_minutes, passenger_count
        
    Returns:
        DataFrame with daily aggregated metrics by zone
        
    Example:
        >>> facts = spark.readStream.table("fact_taxi_trips")
        >>> zones = spark.read.table("dim_taxi_zones_scd").filter(F.col("__END_AT").isNull())
        >>> enriched = enrich_with_pickup_zone(facts, zones)
        >>> daily_agg = aggregate_daily_revenue_by_zone(enriched)
    """
    return (
        df
        .groupBy("trip_date", "pickup_borough", "pickup_zone", "pickup_location_id")
        .agg(
            F.sum("fare_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.count("*").alias("trip_count"),
            F.sum("trip_distance").alias("total_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.sum("passenger_count").alias("total_passengers")
        )
        .withColumns({
            "revenue_per_mile": F.round(F.col("total_revenue") / F.col("total_distance"), 2)
        })
    )
