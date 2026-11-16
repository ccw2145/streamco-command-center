# Owner: Stream A (Data + Pipelines). cc_stream gold: per-minute viewing quality by cell.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CELL = ["cdn", "metro", "device_family", "app_version"]


@dp.materialized_view(name="gold_session_drops", cluster_by=["minute_ts"],
                      comment="Sessions started per minute and cell (weighted) and the share that ended in a drop.")
def gold_session_drops():
    s = spark.read.table("silver_sessions")  # noqa: F821
    return (s.groupBy(F.date_trunc("minute", "start_ts").alias("minute_ts"), *CELL)
            .agg(F.sum("sample_weight").alias("sessions_weighted"),
                 F.sum(F.col("sample_weight") * F.col("dropped").cast("double")).alias("drops_weighted"))
            .withColumn("drop_rate", F.col("drops_weighted") / F.col("sessions_weighted")))


@dp.materialized_view(name="gold_cdn_region_minute", cluster_by=["minute_ts"],
                      comment="Viewing quality per minute x CDN x metro x device x app version. Source of "
                              "mv_viewing_quality and the anomaly engine. freshness_ts = latest arrival in the minute.")
def gold_cdn_region_minute():
    m = spark.read.table("silver_cdn_minute").alias("m")  # noqa: F821
    d = spark.read.table("gold_session_drops").alias("d")  # noqa: F821
    on = [F.col(f"m.{c}").eqNullSafe(F.col(f"d.{c}")) for c in ["minute_ts", *CELL]]
    cond = on[0]
    for c in on[1:]:
        cond = cond & c
    return m.join(d, cond, "left").select(
        "m.minute_ts", "m.cdn", "m.metro", "m.device_family", "m.app_version",
        F.col("m.concurrent_sessions").cast("bigint").alias("concurrent_sessions"), "m.rebuffer_ratio",
        "m.start_failure_rate", F.col("d.drop_rate").alias("session_drop_rate"), "m.freshness_ts")
