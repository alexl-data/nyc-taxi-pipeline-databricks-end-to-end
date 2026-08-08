"""
Unit Tests for NYC Taxi Pipeline Transformations
=================================================
These tests run locally without a Databricks cluster and validate business logic.

Design Decisions:
1. PySpark Local Session: Tests don't need cloud cluster
2. Fixture-based test data: Reproducible, isolated tests
3. Tests import actual production code (transformations.py)
-> Software Engineering best practice:
    - Separation of Concerns: Business logic (transformations.py) is decoupled
    from orchestration (e.g. silver_layer.py with @dp decorators)
    - Single Source of Truth: Changes to production logic automatically flow to tests
    - Regression Detection: Tests catch real bugs, not just test-specific issues
4. pytest framework: Standard in Python community

These tests would run in the CI workflow (.github/workflows/ci.yml).
"""

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Import actual production code (testable business logic)
from transformations import (
    aggregate_daily_revenue_by_zone,
    apply_data_quality_filters,
    calculate_trip_metrics,
    enrich_with_pickup_zone,
    standardize_trip_schema,
)


@pytest.fixture(scope="session")
def spark():
    """
    Creates a local PySpark session for tests.
    Cleanup after test suite.
    """
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("nyc_taxi_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_trip_data(spark):
    """
    Generates sample trip data for tests.
    Contains both valid and invalid rows.
    """
    schema = StructType(
        [
            StructField("vendor_id", StringType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("pickup_location_id", IntegerType(), True),
            StructField("dropoff_location_id", IntegerType(), True),
        ]
    )

    now = datetime.now(timezone.utc)

    data = [
        # Valid trips
        ("1", now, now + timedelta(minutes=15), 2, 5.5, 18.50, 100, 200),
        ("2", now, now + timedelta(minutes=30), 1, 12.0, 35.00, 150, 250),
        # Invalid trips (should be filtered by expectations)
        (
            "3",
            now,
            now + timedelta(minutes=10),
            0,
            3.0,
            10.00,
            100,
            200,
        ),  # passenger_count = 0
        (
            "4",
            now,
            now + timedelta(minutes=20),
            2,
            -1.5,
            20.00,
            100,
            200,
        ),  # negative distance
        (
            "5",
            now,
            now + timedelta(minutes=25),
            2,
            8.0,
            -5.00,
            100,
            200,
        ),  # negative fare
        (
            "6",
            now + timedelta(minutes=30),
            now,
            2,
            5.0,
            15.00,
            100,
            200,
        ),  # dropoff before pickup
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_zone_data(spark):
    """
    Generates sample zone data for tests.
    """
    schema = StructType(
        [
            StructField("location_id", IntegerType(), True),
            StructField("borough", StringType(), True),
            StructField("zone", StringType(), True),
            StructField("service_zone", StringType(), True),
        ]
    )

    data = [
        (100, "Manhattan", "Midtown", "Yellow Zone"),
        (150, "Manhattan", "Upper West Side", "Yellow Zone"),
        (200, "Brooklyn", "Williamsburg", "Boro Zone"),
        (250, "Queens", "Astoria", "Boro Zone"),
    ]

    return spark.createDataFrame(data, schema)


# ============================================================================
# TESTS: Business Logic Validations
# ============================================================================


def test_trip_duration_calculation(sample_trip_data):
    """
    Test: Calculation of trip_duration_minutes using ACTUAL production code.
    Expected: Correct difference between dropoff and pickup in minutes.
    """
    # Use the REAL transformation function from production code
    result = calculate_trip_metrics(sample_trip_data)

    # First valid row: 15 minutes
    first_row = result.filter(F.col("vendor_id") == "1").first()
    assert (
        abs(first_row["trip_duration_minutes"] - 15.0) < 0.1
    ), "Trip duration should be 15 minutes"

    # Second valid row: 30 minutes
    second_row = result.filter(F.col("vendor_id") == "2").first()
    assert (
        abs(second_row["trip_duration_minutes"] - 30.0) < 0.1
    ), "Trip duration should be 30 minutes"


def test_speed_calculation(sample_trip_data):
    """
    Test: Calculation of speed_mph (miles per hour) using ACTUAL production code.
    Expected: speed = distance / (duration_hours)
    """
    # Use the REAL transformation function from production code
    result = calculate_trip_metrics(sample_trip_data)

    # First row: 5.5 miles in 15 min = 22 mph
    first_row = result.filter(F.col("vendor_id") == "1").first()
    expected_speed = 5.5 / (15.0 / 60.0)  # 22 mph
    assert (
        abs(first_row["speed_mph"] - expected_speed) < 0.1
    ), f"Speed should be {expected_speed} mph"


def test_schema_standardization(spark):
    """
    Test: Standardization of raw Yellow Taxi schema to normalized schema using ACTUAL production code.
    Expected: Raw column names (tpep_pickup_datetime, PULocationID, VendorID) are transformed
              to normalized names (pickup_datetime, pickup_location_id, vendor_id).

    This tests the Bronze→Silver schema transformation that bridges raw data to clean data.
    """
    # Create sample data with RAW Yellow Taxi schema (exactly as in source files)
    raw_schema = StructType(
        [
            StructField("VendorID", StringType(), True),
            StructField(
                "tpep_pickup_datetime", StringType(), True
            ),  # Raw: string timestamp
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("trip_distance", StringType(), True),  # Raw: string double
            StructField("fare_amount", StringType(), True),
            StructField("passenger_count", StringType(), True),  # Raw: string int
            StructField("PULocationID", StringType(), True),
            StructField("DOLocationID", StringType(), True),
        ]
    )

    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    raw_data = [
        ("1", now_str, now_str, "5.5", "18.50", "2", "100", "200"),
    ]

    raw_df = spark.createDataFrame(raw_data, raw_schema)

    # Use the REAL transformation function from production code
    result = standardize_trip_schema(raw_df)

    # Verify normalized column names exist
    result_columns = result.columns
    assert (
        "pickup_datetime" in result_columns
    ), "Should have normalized pickup_datetime column"
    assert (
        "dropoff_datetime" in result_columns
    ), "Should have normalized dropoff_datetime column"
    assert "vendor_id" in result_columns, "Should have normalized vendor_id column"
    assert (
        "pickup_location_id" in result_columns
    ), "Should have normalized pickup_location_id column"
    assert (
        "dropoff_location_id" in result_columns
    ), "Should have normalized dropoff_location_id column"

    # Verify data types are correctly cast
    first_row = result.first()
    assert (
        first_row["pickup_datetime"] is not None
    ), "pickup_datetime should be non-null"
    assert isinstance(
        first_row["trip_distance"], float
    ), "trip_distance should be double/float"
    assert isinstance(
        first_row["fare_amount"], float
    ), "fare_amount should be double/float"
    assert isinstance(
        first_row["passenger_count"], int
    ), "passenger_count should be int"
    assert isinstance(
        first_row["pickup_location_id"], int
    ), "pickup_location_id should be int"
    assert isinstance(first_row["vendor_id"], str), "vendor_id should be string"


def test_data_quality_expectations(sample_trip_data):
    """
    Test: Validation of data quality rules using ACTUAL production code.
    Expected: Only 2 of 6 rows should pass all expectations.

    CRITICAL: This tests the ACTUAL @dp.expect_or_drop rules from silver_layer.py:
    1. valid_trip_distance: 0 < trip_distance < 1000
    2. valid_fare: 0 < fare_amount < 10000
    3. valid_passenger_count: 1-8 people
    4. valid_timestamps: pickup before dropoff
    5. no_nulls_in_key_fields: No nulls in timestamps

    NOTE: There is NO rate_code_id validation in the actual pipeline.
    """
    # Use the REAL data quality filter function from production code
    validated = apply_data_quality_filters(sample_trip_data)

    valid_count = validated.count()
    total_count = sample_trip_data.count()

    assert (
        valid_count == 2
    ), f"Expected 2 valid rows (vendors 1 and 2), but found {valid_count}"
    assert total_count == 6, f"Test data should have 6 rows, but found {total_count}"

    # Validate that correct rows passed through
    valid_vendors = [row["vendor_id"] for row in validated.collect()]
    assert (
        "1" in valid_vendors and "2" in valid_vendors
    ), "Vendor 1 and 2 should be valid"


def test_zone_join_enrichment(sample_trip_data, sample_zone_data):
    """
    Test: Join of trips with zone dimensions using ACTUAL production code.
    Expected: Trips should have pickup_borough and pickup_zone.

    CRITICAL: The actual gold_daily_revenue_by_zone only joins PICKUP zone,
    not dropoff zone. This test now matches the real implementation.
    """
    # Use the REAL enrichment function from production code
    enriched = enrich_with_pickup_zone(sample_trip_data, sample_zone_data)

    # Check that pickup borough information was added
    result = enriched.filter(F.col("vendor_id") == "1").first()
    assert result["pickup_borough"] == "Manhattan", "Pickup borough should be Manhattan"
    assert result["pickup_zone"] == "Midtown", "Pickup zone should be Midtown"

    # Verify second trip has correct pickup zone
    result2 = enriched.filter(F.col("vendor_id") == "2").first()
    assert (
        result2["pickup_borough"] == "Manhattan"
    ), "Second trip pickup borough should be Manhattan"
    assert (
        result2["pickup_zone"] == "Upper West Side"
    ), "Second trip pickup zone should be Upper West Side"


def test_daily_aggregation(sample_trip_data, sample_zone_data):
    """
    Test: Daily aggregation for Gold layer using ACTUAL production code.
    Expected: Two separate aggregation groups (one per pickup zone).

    This tests the ACTUAL gold_daily_revenue_by_zone aggregation logic.

    IMPORTANT: This test mirrors the real pipeline flow:
    1. Silver layer filters invalid rows (@dp.expect_or_drop)
    2. Gold layer aggregates ONLY validated data
    3. Aggregation groups by pickup_zone (among other keys)

    The two valid rows have DIFFERENT pickup zones:
    - Vendor 1: pickup_location_id=100 → "Midtown"
    - Vendor 2: pickup_location_id=150 → "Upper West Side"

    Therefore, we expect TWO aggregation groups, each with trip_count=1.
    This tests that the groupBy logic correctly separates different zones.
    """
    # Prepare data: add trip_date and trip_duration_minutes (needed for aggregation)
    prepared = calculate_trip_metrics(sample_trip_data)

    # Apply data quality filters (mirrors Silver layer @dp.expect_or_drop)
    # In production, gold_daily_revenue_by_zone only sees validated data from Silver
    validated = apply_data_quality_filters(prepared)

    # Verify we only have 2 valid rows (like in production)
    assert (
        validated.count() == 2
    ), "Should have 2 valid rows after filtering (like in production)"

    # Enrich with zone information (needed for aggregation by zone)
    enriched = enrich_with_pickup_zone(validated, sample_zone_data)

    # Use the REAL aggregation function from production code
    daily_agg = aggregate_daily_revenue_by_zone(enriched)

    results = daily_agg.collect()

    # CRITICAL: The two valid rows have DIFFERENT pickup zones:
    # - Vendor 1: pickup_location_id=100 → "Midtown"
    # - Vendor 2: pickup_location_id=150 → "Upper West Side"
    # Therefore, aggregation creates TWO groups (not one), each with trip_count=1
    assert (
        len(results) == 2
    ), f"Should have 2 aggregation groups (different zones), but found {len(results)}"

    # Verify each group has exactly 1 trip (tests groupBy logic)
    for row in results:
        assert (
            row["trip_count"] == 1
        ), f"Each zone group should have 1 trip, but {row['pickup_zone']} has {row['trip_count']}"
        assert row["total_revenue"] > 0, "Total revenue should be > 0"
        assert row["avg_fare"] > 0, "Average fare should be > 0"
        assert row["pickup_borough"] == "Manhattan", "Both trips are in Manhattan"
        assert row["pickup_zone"] in [
            "Midtown",
            "Upper West Side",
        ], f"Zone should be Midtown or Upper West Side, got {row['pickup_zone']}"

    # Verify both zones are present (tests that grouping didn't merge them incorrectly)
    zones = {row["pickup_zone"] for row in results}
    assert zones == {
        "Midtown",
        "Upper West Side",
    }, f"Should have both zones separated, got {zones}"


def test_revenue_per_mile_calculation(sample_trip_data, sample_zone_data):
    """
    Test: Calculation of revenue per mile (Gold layer metric) using ACTUAL production code.
    Expected: revenue / distance should be calculated correctly.

    This tests the revenue_per_mile calculation within aggregate_daily_revenue_by_zone.

    IMPORTANT: This test mirrors the real pipeline flow:
    1. Silver layer filters invalid rows (@dp.expect_or_drop)
    2. Gold layer aggregates ONLY validated data

    Therefore, we apply data quality filters BEFORE aggregation.
    """
    # Prepare data: add trip_date and trip_duration_minutes (needed for aggregation)
    prepared = calculate_trip_metrics(sample_trip_data)

    # Apply data quality filters (mirrors Silver layer @dp.expect_or_drop)
    # In production, gold_daily_revenue_by_zone only sees validated data from Silver
    validated = apply_data_quality_filters(prepared)

    # Enrich with zone information (needed for aggregation by zone)
    enriched = enrich_with_pickup_zone(validated, sample_zone_data)

    # Use the REAL aggregation function from production code
    result = aggregate_daily_revenue_by_zone(enriched)

    first_row = result.first()

    # Revenue per mile should be positive and realistic
    assert first_row["revenue_per_mile"] > 0, "Revenue per mile should be > 0"
    assert (
        first_row["revenue_per_mile"] < 100
    ), "Revenue per mile should be realistic (< $100)"

    # Verify calculation: revenue_per_mile = total_revenue / total_distance
    expected_rpm = round(first_row["total_revenue"] / first_row["total_distance"], 2)
    assert (
        abs(first_row["revenue_per_mile"] - expected_rpm) < 0.01
    ), f"Revenue per mile calculation should match: {first_row['revenue_per_mile']} vs {expected_rpm}"


# ============================================================================
# TESTS: Edge Cases & Error Handling
# ============================================================================


def test_zero_duration_trips_handled(spark):
    """
    Test: Trips with 0 seconds duration (pickup == dropoff) using ACTUAL production code.
    Expected: Speed should be 0, not NULL or division-by-zero.

    This tests the production code's robustness for edge cases.
    """
    schema = StructType(
        [
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
        ]
    )

    now = datetime.now(timezone.utc)
    data = [(5.0, now, now)]  # Same pickup and dropoff time

    df = spark.createDataFrame(data, schema)

    # Use the REAL transformation function from production code
    result = calculate_trip_metrics(df)

    row = result.first()
    assert (
        row["speed_mph"] == 0
    ), "Speed should be 0 for zero-duration trips (graceful handling of division by zero)"


def test_null_handling_in_aggregations(spark):
    """
    Test: NULL values in aggregations should be ignored.
    """
    schema = StructType(
        [
            StructField("zone_id", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
        ]
    )

    data = [
        (1, 10.0),
        (1, None),
        (1, 20.0),
    ]

    df = spark.createDataFrame(data, schema)

    result = (
        df.groupBy("zone_id")
        .agg(
            F.sum("fare_amount").alias("total_revenue"),
            F.count("fare_amount").alias("non_null_count"),
        )
        .first()
    )

    assert result["total_revenue"] == 30.0, "Sum should ignore NULLs"
    assert result["non_null_count"] == 2, "Count should only count non-NULL values"


# ============================================================================
# Pytest Configuration
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
