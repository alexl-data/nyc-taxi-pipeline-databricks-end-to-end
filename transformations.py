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
