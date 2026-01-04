# =============================================
# Project 3: Glue ETL Job (Bronoze to Silver)
# =============================================
 
import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, regexp_replace, trim,
    to_timestamp, to_date, date_format
)

# -------------------------
# Initialize Glue Context
# -------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------
# Logging Configuration
# -------------------------
logger = logging.getLogger("project3-bronze-silver")
logger.setLevel(logging.INFO)
logger.info("Starting Glue job: Bronze → Silver")

# -------------------------
# S3 Paths
# -------------------------
BRONZE_BASE = "s3://project3-bronze-bucket/raw-data/"
SILVER_BASE = "s3://project3-silver-bucket/"

# -------------------------
# Utility: normalize column names
# -------------------------
def normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    return df

# =========================
# dim_date
# =========================
logger.info("💥 Spark session initialized")
try:
    logger.info("Processing dim_date")

    dim_date = spark.read.parquet(f"{BRONZE_BASE}dbo_date_dim/")
    dim_date = normalize_columns(dim_date)

    # Deduplicate on primary key
    dim_date = dim_date.dropDuplicates(["date_key"])

    dim_date.write.mode("overwrite").parquet(
        f"{SILVER_BASE}dim_date/"
    )

    logger.info("✅️ dim_date written successfully")

except Exception as e:
    logger.error("❌ Failed processing dim_date", exc_info=True)
    raise e

# =========================
# fact_orders (order_items)
# =========================
try:
    logger.info("Processing fact_orders")

    fact_orders = spark.read.parquet(f"{BRONZE_BASE}dbo_order_items/")
    fact_orders = normalize_columns(fact_orders)

    # Remove records with NULL primary key
    fact_orders = fact_orders.filter(col("lineitem_id").isNotNull())

    # Deduplicate on primary key
    fact_orders = fact_orders.dropDuplicates(["lineitem_id"])

    # ---- Timestamp handling ----
    # Parse ISO timestamp (keep milliseconds)
    fact_orders = fact_orders.withColumn(
        "creation_ts_utc",
        to_timestamp(
            trim(
                regexp_replace(col("creation_time_utc"), "[TZ]", " ")
            ),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    )

    # Derive DATE
    fact_orders = fact_orders.withColumn(
        "creation_date", to_date(col("creation_ts_utc"))
    )

    # Derive TIME (NO milliseconds)
    fact_orders = fact_orders.withColumn(
        "creation_time",
        date_format(col("creation_ts_utc"), "HH:mm:ss")
    )

    # Drop original raw column
    fact_orders = fact_orders.drop("creation_time_utc")

    # ---- Reorder columns: append new ones at the end ----
    base_cols = [
        c for c in fact_orders.columns
        if c not in ["creation_ts_utc", "creation_date", "creation_time"]
    ]

    fact_orders = fact_orders.select(
        *base_cols,
        "creation_ts_utc",
        "creation_date",
        "creation_time"
    )

    fact_orders.write.mode("overwrite").parquet(
        f"{SILVER_BASE}fact_orders/"
    )

    logger.info("✅️ fact_orders written successfully")

except Exception as e:
    logger.error("❌ Failed processing fact_orders", exc_info=True)
    raise e

# =========================
# fact_options (order_item_options)
# =========================
try:
    logger.info("Processing fact_options")

    fact_options = spark.read.parquet(
        f"{BRONZE_BASE}dbo_order_item_options/"
    )
    fact_options = normalize_columns(fact_options)

    # Deduplicate on composite primary key (~2299 rows)
    fact_options = fact_options.dropDuplicates([
        "lineitem_id",
        "option_group_name",
        "option_name"
    ])

    fact_options.write.mode("overwrite").parquet(
        f"{SILVER_BASE}fact_options/"
    )

    logger.info("✅️ fact_options written successfully")

except Exception as e:
    logger.error("❌ Failed processing fact_options", exc_info=True)
    raise e

# -------------------------
# Job Complete
# -------------------------
logger.info("✅️ Glue job project3-bronze-silver completed successfully")
