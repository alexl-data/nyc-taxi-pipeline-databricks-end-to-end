"""
Gold Layer - Star Schema: Facts & Dimensions
=============================================
Responsibility: Production-ready dimensional model for analytical queries

Design Decisions:
1. Star Schema Architecture (Complete Kimball Methodology):
   - fact_taxi_trips: Central fact table with trip details (grain: one trip)
   - dim_date: Time dimension (standard in every star schema)
   - dim_taxi_zones_scd: Location dimension with SCD Type 2
   - dim_vendor: Vendor/service provider dimension
   - Benefits: Maximum analytical flexibility, ad-hoc queries, industry standard
   
2. Fact Table Design:
   - Detail-level facts (one row per trip) instead of pre-aggregated
   - Enables any aggregation at query time (GROUP BY flexibility)
   - Foreign keys to all dimensions (date, zones, vendor)
   - Measures: fare_amount, trip_distance, duration, passenger_count
   
3. Why Star Schema over pre-aggregated tables?
   - Flexibility: Supports any future analytical question
   - Best Practice: Industry standard (Kimball methodology)
   - Ad-hoc Analysis: Business analysts can freely explore
   - Trade-off: Aggregations computed at query time (vs. pre-computed)
   - Mitigation: Delta Lake optimization + optional aggregated tables for dashboards
   
4. Dimension Design:
   - dim_date: Static dimension (no SCD needed) with calendar attributes
   - dim_taxi_zones_scd: SCD Type 2 for tracking zone changes over time
   - dim_vendor: Static dimension with vendor metadata
   
5. Optional Aggregated Tables:
   - gold_daily_revenue_by_zone: Pre-aggregated for performance-critical dashboards
   - Deliberate denormalization: Joins dimensions + aggregates for fast queries

"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, BooleanType
from pyspark import pipelines as dp
from datetime import datetime, timedelta
import holidays as holidays_lib

# Import reusable transformation functions (testable business logic)
from transformations import enrich_with_pickup_zone, aggregate_daily_revenue_by_zone


# ============================================================================
# DIMENSION: Date Dimension (Static)
# ============================================================================

@dp.materialized_view(
    comment="Gold layer: Date dimension for time-based analysis. Static dimension in star schema."
)
def dim_date() -> DataFrame:
    """
    Date dimension table with calendar attributes.
    
    Why this dimension is essential:
    -------------------------------
    - Standard in EVERY star schema (Kimball best practice)
    - Enables analysis by: day of week, month, quarter, holidays, weekends
    - Avoids complex date calculations in every query
    - Pre-join this dimension for dashboard performance
    
    Design Notes:
    ------------
    - Static dimension (no SCD needed - dates don't change)
    - Covers 2019-2024: Current data is 2019 only, dimension extended for future years
      (Production best practice: Date dimensions include buffer years for forward compatibility)
    - US Federal holidays: All 11 federal holidays via Python holidays library
      (includes Juneteenth since 2021, Columbus Day, Veterans Day, etc.)
    - Fiscal year = calendar year (adjust if needed)
    - Library-based holidays: Eliminates manual errors, automatically extensible
    
    Grain: One row per day
    
    Attributes:
    - date (PK): The actual date
    - year, quarter, month: Calendar hierarchy
    - week_of_year: ISO week number
    - day_of_week: 1=Monday, 7=Sunday
    - day_name: "Monday", "Tuesday", etc.
    - is_weekend: Boolean flag
    - is_holiday: Boolean for US federal holidays
    - holiday_name: Name of holiday (NULL if not a holiday)
    - fiscal_quarter, fiscal_year: For fiscal reporting
    
    Use Cases:
    - "Revenue by day of week" → GROUP BY d.day_name
    - "Weekend vs. weekday patterns" → WHERE d.is_weekend = true
    - "Holiday surges" → WHERE d.is_holiday = true
    - "Year-over-year comparison" → GROUP BY d.year, d.quarter
    
    Returns:
        DataFrame with one row per date
    """
    # Generate date range: 2019-01-01 to 2024-12-31 (aligned with data source)
    start_date = datetime(2019, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = [(start_date + timedelta(days=x),) for x in range((end_date - start_date).days + 1)]
    
    # Create DataFrame with date column
    df = spark.createDataFrame(date_range, ["date"])
    
    # US Federal Holidays for 2019-2024
    # Using Python holidays library for complete, accurate, and maintainable holiday data
    # Includes all 11 US Federal holidays: New Year's, MLK, Presidents', Memorial, Juneteenth,
    # Independence, Labor, Columbus, Veterans, Thanksgiving, Christmas
    us_holidays = holidays_lib.US(years=range(2019, 2025))
    holidays = {str(date): name for date, name in us_holidays.items()}
    
    # Add calendar attributes using .withColumns() for better performance
    # (avoids deeply nested execution plans from chained .withColumn() calls)
    return (
        df
        .withColumns({
            "year": F.year("date"),
            "quarter": F.quarter("date"),
            "month": F.month("date"),
            "week_of_year": F.weekofyear("date"),
            "day_of_week": F.dayofweek("date"),  # 1=Sunday, 7=Saturday in Spark
            "day_name": F.date_format("date", "EEEE"),  # "Monday", "Tuesday", etc.
            "day_of_month": F.dayofmonth("date"),
            # Weekend flag (day_of_week: 1=Sunday, 7=Saturday)
            "is_weekend": F.col("day_of_week").isin([1, 7]),
            # Holiday flag
            "is_holiday": F.col("date").cast("string").isin(list(holidays.keys())),
            "holiday_name": F.when(
                F.col("is_holiday"),
                F.create_map([F.lit(x) for pair in holidays.items() for x in pair])
                    .getItem(F.col("date").cast("string"))
            ).otherwise(F.lit(None)),
            # Fiscal year attributes (assuming fiscal year = calendar year)
            "fiscal_year": F.col("year"),
            "fiscal_quarter": F.col("quarter"),
            # Month name for readability
            "month_name": F.date_format("date", "MMMM")
        })
        
        .select(
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "week_of_year",
            "day_of_week",
            "day_name",
            "day_of_month",
            "is_weekend",
            "is_holiday",
            "holiday_name",
            "fiscal_year",
            "fiscal_quarter"
        )
    )


# ============================================================================
# DIMENSION: Vendor Dimension (Static)
# ============================================================================

@dp.materialized_view(
    comment="Gold layer: Vendor dimension for taxi service providers. Static reference dimension."
)
def dim_vendor() -> DataFrame:
    """
    Vendor dimension table for taxi service providers.
    
    Why this dimension?
    ------------------
    - Separates vendor attributes from fact table (normalization)
    - Enables vendor-level analysis: "Which vendor has higher average fares?"
    - Small static dimension (only 2 vendors in NYC taxi data)
    - Demonstrates dimensional modeling best practices
    
    Design Notes:
    ------------
    - Static dimension (vendors don't change frequently)
    - No SCD needed for this use case
    - Could extend with: contract_start_date, contact_info, technology_platform
    
    Grain: One row per vendor
    
    Attributes:
    - vendor_id (PK): Unique vendor identifier (1, 2)
    - vendor_name: Full company name
    - vendor_code: Short code (CMT, VTS)
    - description: Business description
    
    NYC Taxi Vendors:
    - 1 = Creative Mobile Technologies (CMT)
    - 2 = VeriFone Inc. (VTS)
    
    Use Cases:
    - "Compare revenue by vendor" → JOIN fact_taxi_trips f JOIN dim_vendor v ON f.vendor_id = v.vendor_id
    - "Vendor market share" → GROUP BY v.vendor_name
    - "Performance metrics by vendor" → WHERE v.vendor_code = 'CMT'
    
    Returns:
        DataFrame with one row per vendor
    """
    # Static vendor data (from NYC TLC documentation)
    vendor_data = [
        (1, "Creative Mobile Technologies, LLC", "CMT", 
         "Technology provider for taxi meters and payment systems"),
        (2, "VeriFone Inc.", "VTS", 
         "VeriFone Transportation Systems - payment and dispatch solutions")
    ]
    
    schema = StructType([
        StructField("vendor_id", IntegerType(), False),
        StructField("vendor_name", StringType(), False),
        StructField("vendor_code", StringType(), False),
        StructField("description", StringType(), True)
    ])
    
    return spark.createDataFrame(vendor_data, schema)


# ============================================================================
# FACT TABLE: Detail-level trip facts
# ============================================================================

@dp.table(
    comment="Gold layer: Fact table with detail-level trip data. Central table in star schema."
)
def fact_taxi_trips() -> DataFrame:
    """
    Central fact table in star schema with one row per trip.
    
    Grain: One taxi trip
    
    Fact Measures:
    - fare_amount: Trip fare in USD
    - trip_distance: Distance in miles
    - trip_duration_minutes: Duration in minutes
    - passenger_count: Number of passengers
    - speed_mph: Average speed
    
    Dimension Foreign Keys:
    - trip_date → dim_date
    - pickup_location_id → dim_taxi_zones_scd
    - dropoff_location_id → dim_taxi_zones_scd
    - vendor_id → dim_vendor
    
    Design Notes:
    - Detail-level facts enable any aggregation at query time
    - Only foreign keys stored (normalized design)
    - Dimension attributes (borough, zone names) obtained via JOIN at query time
    - For point-in-time queries with SCD: Join on date BETWEEN __START_AT AND __END_AT
    
    Use Cases:
    - "Average fare by day of week" → JOIN dim_date ON trip_date
    - "Weekend revenue by vendor" → JOIN dim_date + dim_vendor
    - "Holiday vs. non-holiday patterns" → JOIN dim_date WHERE is_holiday
    - "Borough-to-borough flows" → Self-join on dim_taxi_zones_scd (pickup + dropoff)
    
    Returns:
        DataFrame with trip facts + dimension foreign keys
    """
    return (
        spark.readStream.table("silver_taxi_trips")  # Streaming! Maintains streaming chain from Bronze
        .select(
            # Time dimension FK
            F.col("trip_date"),  # Already computed in Silver
            
            # Time attributes (for convenience, even though dim_date has these)
            F.col("pickup_datetime"),
            F.col("dropoff_datetime"),
            F.col("pickup_hour"),  # Already computed in Silver
            F.col("pickup_day_of_week"),  # Already computed in Silver
            
            # Location dimension FKs (normalized - no zone names!)
            F.col("pickup_location_id"),
            F.col("dropoff_location_id"),
            
            # Vendor dimension FK
            F.col("vendor_id"),
            
            # Fact measures
            F.col("fare_amount"),
            F.col("trip_distance"),
            F.col("trip_duration_minutes"),
            F.col("passenger_count"),
            F.col("speed_mph"),
            
            # Audit columns
            F.col("_ingestion_timestamp"),
            F.col("_source_file")
        )
    )


# ============================================================================
# DIMENSION: Taxi Zones with SCD Type 2
# ============================================================================

# SCD Type 2 dimension table for taxi zones via AUTO CDC FROM SNAPSHOT
#
# Why SCD Type 2 for zones?
# -------------------------
# - Zone boundaries and borough assignments can change over time
# - Point-in-time analysis: "Which borough was zone 50 in on 2019-06-15?"
# - Regulatory compliance: Track historical zone definitions
#
# Design Explanation for Interview:
# --------------------------------
# AUTO CDC FROM SNAPSHOT significantly simplifies SCD Type 2 implementation:
#
# 1. Source: silver_taxi_zones (Materialized View - Batch source)
#    - create_auto_cdc_from_snapshot_flow: For batch/snapshot sources (no CDF needed!)
#    - Compares current snapshot with previous state to detect changes
#    - Works with batch sources that don't have Change Data Feed
#
# 2. create_auto_cdc_from_snapshot_flow() Parameters:
#    - target: Target streaming table name (dim_taxi_zones_scd)
#    - source: Source table name (silver_taxi_zones)
#    - keys: Business key (location_id) - identifies unique zone
#    - stored_as_scd_type: 2 → Keep historical versions (SCD Type 2)
#    - NO sequence_by needed! Snapshot comparison handles ordering automatically
#
# 3. Automatically generated columns:
#    - __START_AT: Valid from (auto-generated timestamp)
#    - __END_AT: Valid until (NULL for current version)
#
# 4. Example use case:
#    "Which borough was zone 50 in on 2023-06-15?"
#    → Query filters on location_id=50 AND '2023-06-15' BETWEEN __START_AT AND __END_AT
#
# 5. Why not manual MERGE?
#    - AUTO CDC handles change detection automatically
#    - No custom SQL merge logic needed
#    - Event log shows applied changes for monitoring
#    - Robust against duplicate events (idempotent)
#
# Joining with Facts:
# ------------------
# -- Current state join (most common):
# SELECT f.*, d.borough, d.zone
# FROM fact_taxi_trips f
# JOIN dim_taxi_zones_scd d 
#   ON f.pickup_location_id = d.location_id 
#   AND d.__END_AT IS NULL
#
# -- Point-in-time join (historical analysis):
# SELECT f.*, d.borough, d.zone
# FROM fact_taxi_trips f
# JOIN dim_taxi_zones_scd d 
#   ON f.pickup_location_id = d.location_id 
#   AND f.trip_date BETWEEN d.__START_AT AND COALESCE(d.__END_AT, CURRENT_DATE())

# Create target streaming table for SCD Type 2 dimension
dp.create_streaming_table(
    name="dim_taxi_zones_scd",
    comment="Gold layer: Taxi zones dimension with SCD Type 2 history tracking via AUTO CDC FROM SNAPSHOT."
)

# Configure AUTO CDC FROM SNAPSHOT flow
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_taxi_zones_scd",
    source="silver_taxi_zones",
    keys=["location_id"],
    stored_as_scd_type=2
)


# ============================================================================
# OPTIONAL: Pre-aggregated Table for Performance-Critical Dashboards
# ============================================================================

@dp.table(
    comment="Gold layer: Pre-aggregated daily revenue by zone for performance-critical dashboards. "
            "Optional table - fact_taxi_trips can answer the same questions with more flexibility."
)
def gold_daily_revenue_by_zone() -> DataFrame:
    """
    Pre-aggregated daily revenue metrics by zone.
    
    Why have this when fact_taxi_trips exists?
    -----------------------------------------
    Trade-off: Storage vs. Query Performance
    
    - fact_taxi_trips: 10M+ rows → Flexible but slower aggregation queries
    - gold_daily_revenue_by_zone: ~10K rows → Fast queries, limited flexibility
    
    Design Decision: Deliberate Denormalization
    - fact_taxi_trips: Normalized (only location_id foreign keys)
    - gold_daily_revenue_by_zone: Denormalized (includes borough, zone names)
    - This table pre-computes: JOIN + GROUP BY → stored result
    
    Use When:
    - Executive dashboard with known queries (e.g., "daily revenue by zone")
    - Query runs frequently (every 5 minutes auto-refresh)
    - Sub-second response time required
    - Data freshness: Daily is sufficient (not real-time)
    
    Don't Use When:
    - Ad-hoc analysis (use fact_taxi_trips instead)
    - Query needs different granularity (e.g., hourly, by vendor)
    - Storage cost is a concern
    
    Pattern:
    - Build star schema first (fact + dims)
    - Profile query performance
    - Add aggregated tables only for proven bottlenecks
    
    Returns:
        DataFrame with daily aggregated metrics by zone
    """
    # Join fact table with dimension to get borough/zone names
    # This demonstrates deliberate denormalization for performance
    facts = spark.readStream.table("fact_taxi_trips")
    # Get current zone versions (where __END_AT is NULL = still active)
    zones = spark.read.table("dim_taxi_zones_scd").filter(F.col("__END_AT").isNull())
    
    # Enrich with pickup zone, then aggregate by date and zone
    enriched = enrich_with_pickup_zone(facts, zones)
    
    # NOTE: No watermark needed for aggregation - groupBy keys have bounded cardinality
    #       (~365 days × ~265 zones = ~100k state entries, not unbounded like deduplication)
    #       Different from dropDuplicates which needs watermark for unbounded keys
    return aggregate_daily_revenue_by_zone(enriched)
