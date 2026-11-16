# Owner: Stream A (Data + Pipelines). cc_stream silver: dedupe, lateness, expectations, minute grain.
#
# Dedupe is by event key inside materialized views rather than a streaming watermark: the landing
# zone holds 14 days of history plus replays, and Auto Loader may read those files in any order, so a
# watermark would silently drop older buckets on a full refresh. Lateness is kept as data instead:
# is_late = the row arrived more than LATE_S seconds after its event time.
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

LATE_S = 30
CELL = ["cdn", "metro", "device_family", "app_version"]


@dp.temporary_view(name="cdn_rows_checked")
@dp.expect_or_drop("cdn_and_metro_not_null", "cdn IS NOT NULL AND metro IS NOT NULL")
@dp.expect_or_drop("ratios_between_0_and_1",
                   "rebuffer_ms <= play_ms AND failed_starts <= session_starts AND rebuffer_ms >= 0 "
                   "AND failed_starts >= 0")
@dp.expect("app_version_present", "app_version IS NOT NULL")
def cdn_rows_checked():
    return spark.read.table("bronze_cdn_telemetry")  # noqa: F821


@dp.materialized_view(name="silver_cdn_minute", cluster_by=["minute_ts"],
                      comment="CDN telemetry per minute and cell, deduplicated. concurrent_sessions = viewer-minutes "
                              "per minute (play time / 60 s), so mixed 10 s / 1 min / hourly-snapshot rows add up.")
def silver_cdn_minute():
    first = Window.partitionBy("event_ts", *CELL).orderBy("ingest_ts")
    rows = (spark.read.table("cdn_rows_checked")  # noqa: F821
            .withColumn("_rn", F.row_number().over(first)).where("_rn = 1"))
    return (rows.groupBy(F.date_trunc("minute", "event_ts").alias("minute_ts"), *CELL)
            .agg(F.round(F.sum("play_ms") / 60000.0).cast("int").alias("concurrent_sessions"),
                 F.sum("session_starts").cast("int").alias("session_starts"),
                 F.sum("failed_starts").cast("int").alias("failed_starts"),
                 (F.sum("rebuffer_ms") / F.sum("play_ms")).alias("rebuffer_ratio"),
                 (F.sum("failed_starts") / F.nullif(F.sum("session_starts"), F.lit(0))).alias("start_failure_rate"),
                 F.max(F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts") > LATE_S).alias("is_late"),
                 F.max("ingest_ts").alias("freshness_ts")))


@dp.materialized_view(name="silver_sessions", cluster_by=["start_ts"],
                      comment="One row per playback session. rebuffer_seconds = 10 s per rebuffer heartbeat event.")
def silver_sessions():
    ev = spark.read.table("bronze_session_events").dropDuplicates(["event_id"])  # noqa: F821
    return (ev.groupBy("session_id")
            .agg(F.max("subscriber_id").alias("subscriber_id"), F.max("profile_id").alias("profile_id"),
                 F.min("event_ts").alias("start_ts"), F.max("event_ts").alias("end_ts"),
                 F.max("cdn").alias("cdn"), F.max("metro").alias("metro"),
                 F.max("device_family").alias("device_family"), F.max("app_version").alias("app_version"),
                 (F.sum((F.col("event_type") == "rebuffer").cast("int")) * 10.0).alias("rebuffer_seconds"),
                 F.max(F.col("event_type") == "error").alias("failed_start"),
                 F.max(F.col("event_type") == "drop").alias("dropped"),
                 F.max("sample_weight").alias("sample_weight")))
