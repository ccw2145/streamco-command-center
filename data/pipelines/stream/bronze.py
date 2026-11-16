# Owner: Stream A (Data + Pipelines). cc_stream bronze: Auto Loader over the raw_landing JSON.
# Schema hints pin every contract column's type; anything unexpected lands in _rescued_data.
# Pipeline configuration: landing_path (resources/pipelines.yml) = /Volumes/<catalog>/<schema>/raw_landing.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")  # noqa: F821

CDN_HINTS = ("event_ts TIMESTAMP, cdn STRING, metro STRING, device_family STRING, app_version STRING, "
             "concurrent_sessions INT, session_starts INT, failed_starts INT, rebuffer_ms BIGINT, play_ms BIGINT, "
             "bitrate_kbps_avg INT, ingest_ts TIMESTAMP")
SESSION_HINTS = ("event_id STRING, event_ts TIMESTAMP, session_id STRING, subscriber_id BIGINT, profile_id BIGINT, "
                 "event_type STRING, cdn STRING, metro STRING, device_family STRING, app_version STRING, "
                 "stream_id STRING, sample_weight DOUBLE, ingest_ts TIMESTAMP")


def autoload(subdir: str, hints: str):
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaHints", hints)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.partitionColumns", "")
            .option("pathGlobFilter", "*.json")
            .load(f"{LANDING}/{subdir}/")
            .withColumn("source_system", F.lit("telemetry"))
            .withColumn("_ingested_at", F.current_timestamp()))


@dp.table(name="bronze_cdn_telemetry", comment="Raw CDN telemetry per cell and time bucket (Auto Loader).",
          cluster_by=["event_ts"])
def bronze_cdn_telemetry():
    cols = [c.split()[0] for c in CDN_HINTS.split(", ")]
    return autoload("cdn", CDN_HINTS).select(*cols, F.col("_metadata.file_path").alias("_source_file"),
                                              "_rescued_data", "source_system", "_ingested_at")


@dp.table(name="bronze_session_events", comment="Raw playback session events (Auto Loader).",
          cluster_by=["event_ts"])
def bronze_session_events():
    cols = [c.split()[0] for c in SESSION_HINTS.split(", ")]
    return autoload("sessions", SESSION_HINTS).select(*cols, "_rescued_data", "source_system", "_ingested_at")
