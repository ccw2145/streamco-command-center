# Owner: Stream A (Data + Pipelines). cc_stream: the simulated batch source systems (planted.SOURCE_SYSTEMS).
#
# Each source lands JSON in raw_landing/<source_id>/<entity>/ in its own native shape (written by 04/05):
#   subscriptions  MySQL CDC, camelCase, op I/U, tier codes, DATETIME text      -> bronze_mysql_accounts,
#                                                                                bronze_mysql_subscriptions
#   billing        PostgreSQL CDC, snake_case, amounts in cents, op I/U, _lsn   -> bronze_pg_billing_attempts
#   support        Salesforce Service Cloud cases, PascalCase / __c fields      -> bronze_sfdc_support_cases
#   payments       payment-processor webhook events, nested JSON, epoch seconds -> bronze_processor_events
# Bronze keeps the source shape plus source_system and _ingested_at; silver applies the CDC (latest row
# per key), decodes codes, converts cents to dollars and renames to the lakehouse conventions. The
# governed gold tables and mv_command_center_metrics read only this chain.
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path")  # noqa: F821
TIER = {"SPT": "Sports", "FAM": "Family", "ORG": "Originals", "BDL": "Bundle"}
SRC_TS = "yyyy-MM-dd HH:mm:ss.SSS"


def autoload(source_id: str, entity: str, hints: str):
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaHints", hints)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("pathGlobFilter", "*.json")
            .load(f"{LANDING}/{source_id}/{entity}/")
            .withColumn("source_system", F.lit(source_id))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path")))


def latest(df, key: str, order: str):
    """Apply CDC: keep the newest row per key."""
    w = Window.partitionBy(key).orderBy(F.col(order).desc(), F.col("op").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def decode_tier(col):
    return F.element_at(F.create_map(*[x for k, v in TIER.items() for x in (F.lit(k), F.lit(v))]), F.upper(col))


# ---- bronze ------------------------------------------------------------------------------------------

@dp.table(name="bronze_mysql_accounts", comment="Subscriptions MySQL: accounts table CDC (I/U).")
def bronze_mysql_accounts():
    return autoload("subscriptions", "accounts", "op STRING, accountId BIGINT, subscriberId BIGINT, tierCode STRING, "
                    "signupAt STRING, canceledAt STRING, _commit_ts STRING")


@dp.table(name="bronze_mysql_subscriptions", comment="Subscriptions MySQL: subscription_events CDC (I/U).")
def bronze_mysql_subscriptions():
    return autoload("subscriptions", "subscription_events", "op STRING, eventId BIGINT, subscriberId BIGINT, "
                    "tierCode STRING, eventType STRING, eventAt STRING, isReversed INT, reversedAt STRING, "
                    "_commit_ts STRING")


@dp.table(name="bronze_pg_billing_attempts", comment="Billing PostgreSQL: billing_attempts CDC (I/U), cents.")
def bronze_pg_billing_attempts():
    return autoload("billing", "billing_attempts", "op STRING, attempt_id BIGINT, subscriber_id BIGINT, "
                    "attempt_ts STRING, amount_cents INT, retry_of BIGINT, retry_scheduled BOOLEAN, "
                    "grace_expires_ts STRING, _lsn BIGINT")


@dp.table(name="bronze_sfdc_support_cases", comment="Salesforce Service Cloud: Case records.")
def bronze_sfdc_support_cases():
    return autoload("support", "cases", "Id STRING, Subscriber_Id__c STRING, CreatedDate STRING, "
                    "Is_Showcase__c BOOLEAN")


@dp.table(name="bronze_processor_events", comment="Payment processor webhook events (charge.succeeded / charge.failed).")
def bronze_processor_events():
    return autoload("payments", "processor_events", "id STRING, type STRING, created BIGINT")


# ---- silver ------------------------------------------------------------------------------------------

@dp.materialized_view(name="silver_accounts", comment="Current account state per subscriber (CDC applied).")
@dp.expect_or_drop("subscriber_present", "subscriber_id IS NOT NULL")
def silver_accounts():
    a = latest(spark.read.table("bronze_mysql_accounts"), "accountId", "_commit_ts")  # noqa: F821
    return a.select(
        F.col("subscriberId").alias("subscriber_id"), F.col("accountId").alias("account_id"),
        decode_tier("tierCode").alias("tier"), (F.col("isBundle") == 1).alias("is_bundle"),
        F.to_timestamp("signupAt", SRC_TS).alias("signup_ts"), F.lower("signupChannel").alias("signup_source"),
        F.col("monthlyPrice").cast("decimal(6,2)").alias("plan_price"), F.col("homeMetro").alias("metro"),
        F.col("stateName").alias("state"), F.col("tzName").alias("tz"), F.col("deviceType").alias("device_primary"),
        F.regexp_replace("acquirerId", "^ACQ-", "").alias("acquirer"), F.lower("status").alias("status"),
        F.to_timestamp("canceledAt", SRC_TS).alias("canceled_ts"), (F.col("kidsOnly") == 1).alias("is_kids_only_account"),
        "source_system", "_ingested_at")


@dp.materialized_view(name="silver_subscription_events",
                      comment="Subscription lifecycle events incl. cancel clicks (cancel_request) and reversals.")
@dp.expect_or_drop("event_type_known", "event_type IN ('start', 'cancel', 'pause', 'resume', 'cancel_request')")
def silver_subscription_events():
    e = latest(spark.read.table("bronze_mysql_subscriptions"), "eventId", "_commit_ts")  # noqa: F821
    return e.select(
        F.col("eventId").alias("event_id"), F.col("subscriberId").alias("subscriber_id"),
        decode_tier("tierCode").alias("tier"), F.lower("eventType").alias("event_type"),
        F.to_timestamp("eventAt", SRC_TS).alias("event_ts"),
        F.when(F.col("cancelType") == "VOL", "voluntary").when(F.col("cancelType") == "INVOL", "involuntary_billing")
        .alias("cancel_type"), F.col("reasonCode").alias("cancel_reason_code"), F.lower("outcome").alias("outcome"),
        (F.coalesce(F.col("isReversed"), F.lit(0)) == 1).alias("is_reversed"),
        F.to_timestamp("reversedAt", SRC_TS).alias("reversed_ts"), "source_system", "_ingested_at")


@dp.materialized_view(name="silver_billing_attempts", comment="Payment attempts in dollars (CDC applied).")
@dp.expect_or_drop("amount_non_negative", "amount_usd >= 0")
def silver_billing_attempts():
    b = latest(spark.read.table("bronze_pg_billing_attempts"), "attempt_id", "_lsn")  # noqa: F821
    return b.select(
        "attempt_id", "subscriber_id", F.to_timestamp("attempt_ts", SRC_TS).alias("attempt_ts"), "attempt_type",
        decode_tier("tier_code").alias("tier"), (F.col("amount_cents") / 100).cast("decimal(8,2)").alias("amount_usd"),
        F.upper(F.regexp_replace("acquirer", "^acq_", "")).alias("acquirer"), F.col("status").alias("result"),
        "decline_code", "retry_of", "retry_scheduled", F.to_timestamp("grace_expires_ts", SRC_TS).alias("grace_expires_ts"),
        "source_system", "_ingested_at")


@dp.materialized_view(name="silver_support_cases", comment="Support cases (Salesforce Service Cloud).")
def silver_support_cases():
    c = spark.read.table("bronze_sfdc_support_cases").dropDuplicates(["Id"])  # noqa: F821
    return c.select(
        F.col("Id").alias("case_id"), F.col("CaseNumber").alias("case_number"),
        F.col("Subscriber_Id__c").cast("bigint").alias("subscriber_id"), F.lower("Origin").alias("origin"),
        F.coalesce(F.col("Type"), F.lit("Unclassified")).alias("case_type"), F.col("Subject").alias("subject"),
        F.col("Status").alias("status"), F.coalesce(F.col("Is_Showcase__c"), F.lit(False)).alias("is_showcase"),
        F.to_timestamp("CreatedDate", "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias("created_ts"), "source_system", "_ingested_at")


@dp.materialized_view(name="silver_processor_events", comment="Payment processor charge events, flattened.")
def silver_processor_events():
    p = spark.read.table("bronze_processor_events").dropDuplicates(["id"])  # noqa: F821
    o = "data.object"
    return p.select(
        F.col("id").alias("event_id"), F.col("type").alias("event_type"),
        F.timestamp_seconds("created").alias("created_ts"), F.col(f"{o}.id").alias("charge_id"),
        F.regexp_replace(F.col(f"{o}.id"), "^ch_", "").cast("bigint").alias("attempt_id"),
        F.regexp_replace(F.col(f"{o}.customer"), "^cus_", "").cast("bigint").alias("subscriber_id"),
        (F.col(f"{o}.amount") / 100).cast("decimal(8,2)").alias("amount_usd"), F.col(f"{o}.status").alias("status"),
        F.col(f"{o}.failure_code").alias("failure_code"), F.col(f"{o}.metadata.acquirer").alias("acquirer"),
        F.col(f"{o}.metadata.attempt_type").alias("attempt_type"), "source_system", "_ingested_at")
