"""PySpark medallion pipeline and release-gate validation."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

USER_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("full_name", T.StringType(), False),
        T.StructField("plan_type", T.StringType(), False),
        T.StructField("contract_months", T.IntegerType(), False),
        T.StructField("monthly_fee_usd", T.DoubleType(), False),
        T.StructField("region", T.StringType(), False),
        T.StructField("signup_date", T.DateType(), False),
        T.StructField("status", T.StringType(), False),
    ]
)
CONTENT_SCHEMA = T.StructType(
    [
        T.StructField("content_id", T.StringType(), False),
        T.StructField("title", T.StringType(), False),
        T.StructField("category", T.StringType(), False),
        T.StructField("duration_minutes", T.IntegerType(), False),
        T.StructField("release_year", T.IntegerType(), False),
    ]
)
WATCH_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("content_id", T.StringType(), False),
        T.StructField("session_id", T.StringType(), False),
        T.StructField("event_ts", T.TimestampType(), False),
        T.StructField("device", T.StringType(), False),
        T.StructField("watched_minutes", T.DoubleType(), False),
        T.StructField("ingest_ts", T.TimestampType(), False),
    ]
)
SEARCH_SCHEMA = T.StructType(
    [
        T.StructField("search_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("query", T.StringType(), True),
        T.StructField("result_content_id", T.StringType(), True),
        T.StructField("clicked", T.BooleanType(), False),
        T.StructField("search_ts", T.TimestampType(), False),
        T.StructField("device", T.StringType(), False),
        T.StructField("ingest_ts", T.TimestampType(), False),
    ]
)


def create_spark(app_name: str = "customer360-content-lakehouse") -> SparkSession:
    """Create a quiet local Spark session that is also configurable for a cluster."""
    master = os.getenv("C360_SPARK_MASTER", "local[2]")
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _read_csv(spark: SparkSession, path: Path, schema: T.StructType) -> DataFrame:
    return spark.read.option("header", True).schema(schema).csv(str(path))


def _write_layer(frame: DataFrame, path: Path) -> None:
    frame.write.mode("overwrite").parquet(str(path))


def _export_csv(frame: DataFrame, path: Path, order_by: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.orderBy(*order_by).toPandas().to_csv(path, index=False)


def run_partition(
    spark: SparkSession,
    root: Path,
    event_date: date,
    *,
    export_dir: Path | None = None,
) -> dict[str, object]:
    """Process one date idempotently, rebuild gold snapshots, and return evidence."""
    raw = root / "raw"
    day = event_date.isoformat()
    source = raw / f"event_date={day}"
    if not source.exists():
        raise FileNotFoundError(f"Raw partition does not exist: {source}")

    users = _read_csv(spark, raw / "master" / "users.csv", USER_SCHEMA)
    content = _read_csv(spark, raw / "master" / "content.csv", CONTENT_SCHEMA)
    watch_raw = _read_csv(spark, source / "watch_events.csv", WATCH_SCHEMA)
    search_raw = _read_csv(spark, source / "search_events.csv", SEARCH_SCHEMA)

    lakehouse = root / "lakehouse"
    bronze_watch = watch_raw.withColumn("event_date", F.lit(day).cast("date"))
    bronze_search = search_raw.withColumn("event_date", F.lit(day).cast("date"))
    _write_layer(
        bronze_watch.drop("event_date"),
        lakehouse / "bronze" / "watch_events" / f"event_date={day}",
    )
    _write_layer(
        bronze_search.drop("event_date"),
        lakehouse / "bronze" / "search_events" / f"event_date={day}",
    )

    watch_window = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())
    watch_deduped = (
        bronze_watch.withColumn("_row_number", F.row_number().over(watch_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    watch_joined = (
        watch_deduped.alias("w")
        .join(users.alias("u"), F.col("w.customer_id") == F.col("u.customer_id"), "left")
        .join(content.alias("c"), F.col("w.content_id") == F.col("c.content_id"), "left")
        .select(
            "w.*",
            F.col("u.plan_type"),
            F.col("u.region"),
            F.col("c.title"),
            F.col("c.category"),
            F.col("c.duration_minutes"),
        )
    )
    watch_reason = (
        F.when(F.col("plan_type").isNull(), F.lit("unknown_customer"))
        .when(F.col("title").isNull(), F.lit("unknown_content"))
        .when(F.col("watched_minutes") <= 0, F.lit("non_positive_watch_duration"))
        .when(
            F.col("watched_minutes") > F.col("duration_minutes") * 1.25,
            F.lit("watch_duration_exceeds_tolerance"),
        )
    )
    watch_classified = watch_joined.withColumn("rejection_reason", watch_reason)
    watch_valid = (
        watch_classified.filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason")
        .withColumn(
            "completion_ratio",
            F.least(F.col("watched_minutes") / F.col("duration_minutes"), F.lit(1.0)),
        )
    )
    watch_rejected = watch_classified.filter(F.col("rejection_reason").isNotNull())

    search_window = Window.partitionBy("search_id").orderBy(F.col("ingest_ts").desc())
    search_deduped = (
        bronze_search.withColumn("_row_number", F.row_number().over(search_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    search_joined = (
        search_deduped.alias("s")
        .join(users.alias("u"), F.col("s.customer_id") == F.col("u.customer_id"), "left")
        .join(
            content.alias("c"),
            F.col("s.result_content_id") == F.col("c.content_id"),
            "left",
        )
        .select(
            "s.*",
            F.col("u.plan_type"),
            F.col("u.region"),
            F.col("c.category").alias("result_category"),
        )
    )
    search_reason = (
        F.when(F.col("plan_type").isNull(), F.lit("unknown_customer"))
        .when(F.trim(F.coalesce(F.col("query"), F.lit(""))) == "", F.lit("empty_query"))
        .when(
            F.col("clicked") & F.col("result_category").isNull(),
            F.lit("clicked_result_not_found"),
        )
    )
    search_classified = search_joined.withColumn("rejection_reason", search_reason)
    search_valid = search_classified.filter(F.col("rejection_reason").isNull()).drop(
        "rejection_reason"
    )
    search_rejected = search_classified.filter(F.col("rejection_reason").isNotNull())

    _write_layer(
        watch_valid.drop("event_date"),
        lakehouse / "silver" / "watch_events" / f"event_date={day}",
    )
    _write_layer(
        watch_rejected.drop("event_date"),
        lakehouse / "silver" / "watch_events_rejected" / f"event_date={day}",
    )
    _write_layer(
        search_valid.drop("event_date"),
        lakehouse / "silver" / "search_events" / f"event_date={day}",
    )
    _write_layer(
        search_rejected.drop("event_date"),
        lakehouse / "silver" / "search_events_rejected" / f"event_date={day}",
    )

    all_watch = spark.read.parquet(str(lakehouse / "silver" / "watch_events"))
    all_search = spark.read.parquet(str(lakehouse / "silver" / "search_events"))

    category_totals = all_watch.groupBy("customer_id", "category").agg(
        F.sum("watched_minutes").alias("category_watch_minutes")
    )
    favorite_window = Window.partitionBy("customer_id").orderBy(
        F.col("category_watch_minutes").desc(), F.col("category")
    )
    favorite = (
        category_totals.withColumn("_rank", F.row_number().over(favorite_window))
        .filter(F.col("_rank") == 1)
        .select("customer_id", F.col("category").alias("favorite_category"))
    )
    watch_customer = all_watch.groupBy("customer_id").agg(
        F.count("*").alias("view_events"),
        F.countDistinct("session_id").alias("sessions"),
        F.countDistinct("content_id").alias("unique_titles"),
        F.round(F.sum("watched_minutes"), 2).alias("watch_minutes"),
        F.round(F.avg("completion_ratio"), 4).alias("avg_completion_rate"),
        F.max("event_ts").alias("last_watch_at"),
    )
    search_customer = all_search.groupBy("customer_id").agg(
        F.count("*").alias("searches"),
        F.sum(F.col("clicked").cast("int")).alias("search_clicks"),
        F.max("search_ts").alias("last_search_at"),
    )
    customer_360 = (
        users.join(watch_customer, "customer_id", "left")
        .join(search_customer, "customer_id", "left")
        .join(favorite, "customer_id", "left")
        .fillna(
            {
                "view_events": 0,
                "sessions": 0,
                "unique_titles": 0,
                "watch_minutes": 0.0,
                "avg_completion_rate": 0.0,
                "searches": 0,
                "search_clicks": 0,
                "favorite_category": "No activity",
            }
        )
        .withColumn(
            "engagement_segment",
            F.when(F.col("watch_minutes") >= 500, "Power viewer")
            .when(F.col("watch_minutes") >= 120, "Regular viewer")
            .otherwise("Light viewer"),
        )
    )

    content_kpis = all_watch.groupBy(
        "event_date", "content_id", "title", "category"
    ).agg(
        F.count("*").alias("views"),
        F.countDistinct("customer_id").alias("unique_viewers"),
        F.round(F.sum("watched_minutes"), 2).alias("watch_minutes"),
        F.round(F.avg("completion_ratio"), 4).alias("completion_rate"),
    )
    search_trends = all_search.groupBy(
        "event_date", F.lower(F.trim("query")).alias("query"), "result_category"
    ).agg(
        F.count("*").alias("searches"),
        F.sum(F.col("clicked").cast("int")).alias("clicks"),
        F.round(F.avg(F.col("clicked").cast("double")), 4).alias("click_through_rate"),
    )
    monthly_search = (
        all_search.withColumn("month", F.date_format("search_ts", "yyyy-MM"))
        .groupBy("month", F.lower(F.trim("query")).alias("query"))
        .agg(
            F.count("*").alias("searches"),
            F.sum(F.col("clicked").cast("int")).alias("clicks"),
        )
    )

    for name, frame in (
        ("customer_360", customer_360),
        ("content_kpis", content_kpis),
        ("search_trends", search_trends),
        ("monthly_search_trends", monthly_search),
    ):
        _write_layer(frame, lakehouse / "gold" / name)

    output = export_dir or root.parent / "powerbi" / "exports"
    _export_csv(customer_360, output / "customer_360.csv", ["customer_id"])
    _export_csv(content_kpis, output / "content_kpis.csv", ["event_date", "content_id"])
    _export_csv(search_trends, output / "search_trends.csv", ["event_date", "query"])
    _export_csv(monthly_search, output / "monthly_search_trends.csv", ["month", "query"])

    metrics: dict[str, object] = {
        "status": "PASS",
        "profile": "fixture",
        "seed": 2026,
        "event_date": day,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "watch": {
            "raw": watch_raw.count(),
            "deduplicated": watch_deduped.count(),
            "accepted": watch_valid.count(),
            "quarantined": watch_rejected.count(),
        },
        "search": {
            "raw": search_raw.count(),
            "deduplicated": search_deduped.count(),
            "accepted": search_valid.count(),
            "quarantined": search_rejected.count(),
        },
        "gold": {
            "customer_360": customer_360.count(),
            "content_kpis": content_kpis.count(),
            "search_trends": search_trends.count(),
            "monthly_search_trends": monthly_search.count(),
        },
    }
    assertions = {
        "watch_reconciles": metrics["watch"]["raw"]
        == metrics["watch"]["deduplicated"]
        + (metrics["watch"]["raw"] - metrics["watch"]["deduplicated"]),
        "watch_classification_reconciles": metrics["watch"]["deduplicated"]
        == metrics["watch"]["accepted"] + metrics["watch"]["quarantined"],
        "search_classification_reconciles": metrics["search"]["deduplicated"]
        == metrics["search"]["accepted"] + metrics["search"]["quarantined"],
        "customer_snapshot_nonempty": metrics["gold"]["customer_360"] > 0,
        "content_mart_nonempty": metrics["gold"]["content_kpis"] > 0,
        "search_mart_nonempty": metrics["gold"]["search_trends"] > 0,
    }
    metrics["assertions"] = assertions
    if not all(assertions.values()):
        metrics["status"] = "FAIL"

    report_path = root.parent / "artifacts" / "quality-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    if metrics["status"] != "PASS":
        raise RuntimeError(f"Data-quality gate failed; inspect {report_path}")
    return metrics
