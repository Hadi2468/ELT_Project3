# =============================================
# Project 3: Glue ETL Job (Silver to Gold)
# =============================================

import sys
import logging
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, BooleanType

# =====================================================
# LOGGING
# =====================================================
logger = logging.getLogger("project3-silver-gold")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(handler)

# =====================================================
# SPARK / GLUE
# =====================================================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

logger.info("💥 Spark session initialized")

try:
    # =====================================================
    # READ SILVER TABLES (ONLY)
    # =====================================================
    orders = (
        spark.read.parquet(
            "s3://project3-silver-bucket/fact_orders/"
        )
        .filter(F.col("user_id").isNotNull())
    )

    options = spark.read.parquet(
        "s3://project3-silver-bucket/fact_options/"
    )

    logger.info("✅️ Silver tables loaded successfully")

    # =====================================================
    # ENRICH ORDER REVENUE
    # =====================================================
    options_agg = (
        options
        .groupBy("order_id", "lineitem_id")
        .agg(
            F.sum(
                F.col("option_price") * F.col("option_quantity")
            ).alias("options_revenue")
        )
    )

    orders = (
        orders
        .join(options_agg, ["order_id", "lineitem_id"], "left")
        .withColumn("options_revenue", F.coalesce("options_revenue", F.lit(0.0)))
        .withColumn(
            "total_item_revenue",
            F.col("item_price") * F.col("item_quantity") + F.col("options_revenue")
        )
    )

    # =====================================================
    # FACT_CUSTOMER
    # =====================================================
    base_daily = (
        orders
        .groupBy("user_id", "creation_date")
        .agg(
            F.countDistinct("order_id").alias("orders_daily"),
            F.sum("total_item_revenue").alias("revenue_daily"),
            F.sum(
                F.when(F.col("is_loyalty"), F.col("total_item_revenue"))
                .otherwise(0)
            ).alias("loyalty_revenue"),
            F.count(
                F.when(F.col("is_loyalty"), True)
            ).alias("loyalty_orders")
        )
    )

    # -------------------------
    # RECENCY & CHURN
    # -------------------------
    w_prev = Window.partitionBy("user_id").orderBy("creation_date")

    base_daily = (
        base_daily
        .withColumn("prev_date", F.lag("creation_date").over(w_prev))
        .withColumn(
            "recency_days",
            F.when(F.col("prev_date").isNull(), 0)
             .otherwise(F.datediff("creation_date", "prev_date"))
        )
        .withColumn(
            "churn_risk_flag",
            F.when(F.col("recency_days") > 45, "High")
             .when(F.col("recency_days") > 20, "Medium")
             .otherwise("Low")
        )
        .drop("prev_date")
    )

    # -------------------------
    # CUMULATIVE CLV
    # -------------------------
    w_clv = (
        Window.partitionBy("user_id")
        .orderBy("creation_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    base_daily = base_daily.withColumn(
        "clv_cumulative",
        F.sum("revenue_daily").over(w_clv)
    )

    # -------------------------
    # ROLLING WINDOWS (JOIN-BASED, SAFE)
    # -------------------------
    def rolling_metrics(days):
        l = base_daily.alias("l")
        r = base_daily.alias("r")

        return (
            l.join(
                r,
                (F.col("l.user_id") == F.col("r.user_id")) &
                (F.col("r.creation_date") >= F.date_sub(F.col("l.creation_date"), days - 1)) &
                (F.col("r.creation_date") <= F.col("l.creation_date")),
                "left"
            )
            .groupBy("l.user_id", "l.creation_date")
            .agg(
                F.countDistinct("r.creation_date").alias(f"frequency_{days}d"),
                F.sum("r.revenue_daily").alias(f"monetary_{days}d")
            )
        )

    metrics_7d  = rolling_metrics(7)
    metrics_30d = rolling_metrics(30)
    metrics_90d = rolling_metrics(90)

    fact_customer_daily = (
        base_daily
        .join(metrics_7d,  ["user_id", "creation_date"], "left")
        .join(metrics_30d, ["user_id", "creation_date"], "left")
        .join(metrics_90d, ["user_id", "creation_date"], "left")
    )

    fact_customer_daily.write.mode("overwrite").parquet(
        "s3://project3-gold-bucket/fact_customer/"
    )

    logger.info("fact_customer written")

    # =====================================================
    # FACT_ORDERS_LOCATION
    # =====================================================
    location_daily = (
        orders
        .groupBy("creation_date", "restaurant_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("total_item_revenue").alias("total_revenue"),
            F.avg("total_item_revenue").alias("avg_order_value"),
            F.sum(F.when(F.col("is_loyalty"), F.col("total_item_revenue")).otherwise(0)).alias("loyalty_revenue"),
            F.count(F.when(F.col("is_loyalty"), True)).alias("loyalty_orders"),
            F.sum(F.when(~F.col("is_loyalty"), F.col("total_item_revenue")).otherwise(0)).alias("non_loyalty_revenue"),
            F.count(F.when(~F.col("is_loyalty"), True)).alias("non_loyalty_orders")
        )
    )

    location_daily = (
    location_daily
    .withColumn("period_day",
        F.when(F.col("creation_date").isNotNull(),
               F.dayofmonth("creation_date"))
         .otherwise(F.lit(0))
         .cast(IntegerType())
    )
    .withColumn("period_week",
        F.when(F.col("creation_date").isNotNull(),
               F.weekofyear("creation_date"))
         .otherwise(F.lit(0))
         .cast(IntegerType())
    )
    .withColumn("period_month",
        F.when(F.col("creation_date").isNotNull(),
               F.month("creation_date"))
         .otherwise(F.lit(0))
         .cast(IntegerType())
    )
    .withColumn("year",
        F.when(F.col("creation_date").isNotNull(),
               F.year("creation_date"))
         .otherwise(F.lit(0))
         .cast(IntegerType())
    )
    )

    w_day   = Window.partitionBy("period_day").orderBy(F.desc("total_revenue"))
    w_week  = Window.partitionBy("period_week").orderBy(F.desc("total_revenue"))
    w_month = Window.partitionBy("period_month").orderBy(F.desc("total_revenue"))

    location_daily = (
        location_daily
        .withColumn("revenue_rank_day",   F.rank().over(w_day))
        .withColumn("revenue_rank_week",  F.rank().over(w_week))
        .withColumn("revenue_rank_month", F.rank().over(w_month))
    )

    location_daily.write.mode("overwrite").parquet(
        "s3://project3-gold-bucket/fact_orders_location/"
    )

    logger.info("fact_orders_location written")

except Exception as e:
    logger.error("❌ Glue job failed", exc_info=True)
    raise

# -------------------------
# JOB COMPLETE
# -------------------------
logger.info("✅️ Glue job project3-silver-gold completed successfully")
