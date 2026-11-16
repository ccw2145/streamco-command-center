"""Ground truth for every planted signal (tech-lead fix T12). Owner: Stream 0 (lead).

Single source for the generators (02-05), the validation gate (27), the eval cases (25),
the governance seed (26), the payload builder (28) and scripts/make_frozen_fixture.py.
Never retype these numbers elsewhere: import them. Changes go through the lead.

Units: counts are integers, rates are fractions (0.105 = 10.5%), clock times are ET "HH:MM"
on the game night (variable game_date, default 2027-02-14). Tolerances are absolute unless the
name ends in _REL (relative).
"""
from __future__ import annotations

# --------------------------------------------------------------------------------------
# Replay clock (ET, game night). The UI shows clock labels only, never dates.
# --------------------------------------------------------------------------------------
TZ = "America/New_York"
CLOCK = {
    "t0": "18:00",
    "kickoff": "18:30",
    "halftime_start": "20:15",
    "halftime_end": "20:35",
    "end": "22:30",
}
MORNING_AFTER_AS_OF = "07:00"  # as_of stamp on the day after

# --------------------------------------------------------------------------------------
# Population (design deviation 1)
# --------------------------------------------------------------------------------------
N_SUBSCRIBERS = 4_000_000
N_SPORTS_TIER = 2_500_000
N_PROFILES = 7_000_000
KIDS_PROFILE_SHARE = 0.15
KIDS_ONLY_ACCOUNT_SHARE = 0.035
TIERS = ("Sports", "Family", "Originals", "Bundle")

METROS = {  # metro code -> (display name, lat, lon, tz)
    "NYC": ("New York", 40.71, -74.01, "America/New_York"),
    "LAX": ("Los Angeles", 34.05, -118.24, "America/Los_Angeles"),
    "CHI": ("Chicago", 41.88, -87.63, "America/Chicago"),
    "DFW": ("Dallas-Fort Worth", 32.90, -97.04, "America/Chicago"),
    "HOU": ("Houston", 29.76, -95.37, "America/Chicago"),
    "ATL": ("Atlanta", 33.75, -84.39, "America/New_York"),
    "WAS": ("Washington", 38.91, -77.04, "America/New_York"),
    "PHL": ("Philadelphia", 39.95, -75.17, "America/New_York"),
    "MIA": ("Miami", 25.76, -80.19, "America/New_York"),
    "BOS": ("Boston", 42.36, -71.06, "America/New_York"),
    "PHX": ("Phoenix", 33.45, -112.07, "America/Phoenix"),
    "SEA": ("Seattle", 47.61, -122.33, "America/Los_Angeles"),
    "SFO": ("San Francisco", 37.77, -122.42, "America/Los_Angeles"),
    "DEN": ("Denver", 39.74, -104.99, "America/Denver"),
    "MSP": ("Minneapolis", 44.98, -93.27, "America/Chicago"),
}
METRO_STATE = {"NYC": "New York", "LAX": "California", "CHI": "Illinois", "DFW": "Texas", "HOU": "Texas",
               "ATL": "Georgia", "WAS": "District of Columbia", "PHL": "Pennsylvania", "MIA": "Florida",
               "BOS": "Massachusetts", "PHX": "Arizona", "SEA": "Washington", "SFO": "California",
               "DEN": "Colorado", "MSP": "Minnesota"}

# Sports subscriber share by state (50 states + DC; sums to 1.0). 2024 Census population shares
# with fixed +/-6% noise (seed 20270214), so the order is realistic: CA, TX, FL, NY top.
# dim_subscribers.state must follow this marginal; a subscriber's metro must sit in METRO_STATE[metro].
STATE_SHARE = {
    "California": 0.1190, "Texas": 0.0880, "Florida": 0.0662, "New York": 0.0564,
    "Pennsylvania": 0.0402, "Illinois": 0.0377, "Georgia": 0.0346, "North Carolina": 0.0341,
    "Ohio": 0.0331, "Michigan": 0.0301, "New Jersey": 0.0289, "Virginia": 0.0248, "Washington": 0.0242,
    "Tennessee": 0.0222, "Arizona": 0.0219, "Massachusetts": 0.0205, "Indiana": 0.0192,
    "Missouri": 0.0184, "Wisconsin": 0.0184, "Maryland": 0.0180, "Colorado": 0.0174,
    "Minnesota": 0.0172, "Alabama": 0.0159, "South Carolina": 0.0158, "Louisiana": 0.0141,
    "Kentucky": 0.0140, "Oregon": 0.0125, "Oklahoma": 0.0124, "Connecticut": 0.0113, "Utah": 0.0104,
    "Arkansas": 0.0095, "Nevada": 0.0094, "Iowa": 0.0094, "Kansas": 0.0085, "Mississippi": 0.0085,
    "Idaho": 0.0062, "New Mexico": 0.0059, "Nebraska": 0.0058, "West Virginia": 0.0049,
    "Hawaii": 0.0043, "New Hampshire": 0.0041, "Maine": 0.0039, "Rhode Island": 0.0034,
    "Montana": 0.0033, "Delaware": 0.0030, "South Dakota": 0.0028, "North Dakota": 0.0023,
    "Alaska": 0.0022, "District of Columbia": 0.0021, "Vermont": 0.0019, "Wyoming": 0.0017,
}
STATE_SHARE_TOL_PTS = 0.002   # per-state generated share vs STATE_SHARE


def apportion(total: int, shares: dict[str, float]) -> dict[str, int]:
    """Split an integer total by shares (largest remainder): proportional, sums exactly to total."""
    norm = sum(shares.values())
    raw = {k: total * v / norm for k, v in shares.items()}
    out = {k: int(v) for k, v in raw.items()}
    for k in sorted(raw, key=lambda k: raw[k] - out[k], reverse=True)[: total - sum(out.values())]:
        out[k] += 1
    return out


CDNS = ("CDN-A", "CDN-B", "CDN-C", "CDN-D", "CDN-E", "CDN-F", "CDN-G")
DEVICE_FAMILIES = ("Roku", "FireTV", "SmartTV", "iOS", "Android", "Web")
ROKU_VERSIONS = ("7.0.4", "7.1.2", "7.1.5", "7.2.0")

# --------------------------------------------------------------------------------------
# Scene 0 / Home + Scene 1: halftime surge, demand forecast, capacity
# --------------------------------------------------------------------------------------
PRE_HALFTIME_CONCURRENT = 4_000_000
PEAK_CONCURRENT = 9_500_000
PEAK_TOL_REL = 0.02
SURGE_MULTIPLIER = 2.375          # 4.0M -> 9.5M
SURGE_MULT_TOL = 0.1
SIGNUP_MULTIPLIER = 11.0
SIGNUP_MULT_TOL = 1.0
PAST_EVENT_MULT_RANGE = (2.2, 2.6)
N_PAST_BIG_EVENTS = 8

FORECAST_PEAK_P50 = 9_300_000
FORECAST_PEAK_P10 = 8_900_000
FORECAST_PEAK_P90 = 9_700_000
FORECAST_PEAK_TOL_REL = 0.05      # forecast peak within 5% of actual

HOT_CELL = {"cdn": "CDN-C", "metro": "DFW"}
HOT_CELL_UTILIZATION = 0.94
HOT_CELL_UTIL_TOL = 0.01
OTHER_CELL_UTIL_MAX = 0.80
OTHER_CELL_UTIL_RANGE = (0.55, 0.78)
BACKUP_CELL = {"cdn": "CDN-A", "metro": "DFW"}
BACKUP_CELL_UTILIZATION = 0.69    # 31% headroom
BACKUP_HEADROOM_PCT = 0.31
DFW_CDN_SHARE = {"CDN-C": 0.45, "CDN-A": 0.30, "CDN-B": 0.25}
CDN_C_DFW_PEAK_LOAD = 232_000
CDN_A_DFW_PEAK_LOAD = 156_000
CDN_A_DFW_CAPACITY = 226_000
SHIFT_LOAD = 51_000               # ~22% of CDN-C DFW load (the Roku 7.2.0 slice)

# --------------------------------------------------------------------------------------
# Scene 1: the incident (CDN-C x DFW x Roku 7.2.0)
# --------------------------------------------------------------------------------------
INCIDENT = {
    "alert_id": "ALR-20270214-DFW-C-ROKU720",
    "cdn": "CDN-C",
    "metro": "DFW",
    "device_family": "Roku",
    "app_version": "7.2.0",
    "onset": "20:17",
    "ramp_end": "20:20",
    "shift_approved": "20:26",   # actual track: ops approved the CDN-C -> CDN-A shift
    "resolved": "20:36",
}
INCIDENT_MINUTES_TO_RESOLVE = 19
ROKU_720_SHARE_OF_CELL = 0.22
AFFECTED_SESSIONS = 210_000
REBUFFER_BASELINE = 0.010
REBUFFER_INCIDENT = 0.105         # the Roku 7.2.0 cohort, not the whole cell
REBUFFER_TOL = 0.01
# Whole CDN-C x DFW cell at the incident plateau: only the 22% Roku 7.2.0 slice is failing (~3.1%).
HOT_CELL_REBUFFER_INCIDENT = round(
    REBUFFER_BASELINE * (1 - ROKU_720_SHARE_OF_CELL) + REBUFFER_INCIDENT * ROKU_720_SHARE_OF_CELL, 4)
HOT_CELL_REBUFFER_TOL = 0.005
START_FAIL_BASELINE = 0.018
START_FAIL_INCIDENT = 0.15
SESSION_DROP_LIFT_REL = 0.21
CONTROL_TOL_REL = 0.03
CONTROLS = (
    {"cdn": "CDN-C", "metro": "DFW", "device_family": "Roku", "app_version": "7.1.x"},
    {"cdn": "CDN-A", "metro": "DFW", "device_family": "Roku", "app_version": "7.2.0"},
    {"cdn": "CDN-C", "metro": "HOU", "device_family": "*", "app_version": "*"},
    {"cdn": "CDN-C", "metro": "ATL", "device_family": "*", "app_version": "*"},
)
DECOY = {"cdn": "CDN-F", "metro": "SEA", "rebuffer_lift_pts": 0.008, "start": "20:20", "end": "20:40",
         "window_z": 1.2, "z_max": 2.0}
ANOMALY_RULE = {"z_threshold": 4.0, "persistence_min": 3}
ASSISTED_REPLAY = {"recovery_start_after_approve_min": 2, "recovery_minutes": 4}

# --------------------------------------------------------------------------------------
# Scene 2: cancellations, Sports tier, day after
# --------------------------------------------------------------------------------------
CANCELS_BASELINE = 9_000
CANCELS_OBSERVED = 15_800
CANCELS_EXCESS = 6_800
CANCELS_TOL_REL = 0.01
CANCELS_PCT_OVER = 0.76           # 15,800 / 9,000 - 1, displayed "+76%"
CANCEL_FORECAST_BAND = (8_400, 9_700)
CANCEL_FORECAST_YHAT = 9_050

# driver -> (pct of excess, count, evidence_type, confidence). Order is display order.
CANCEL_DRIVERS = {
    "playback": (0.34, 2_300, "inferred", "high"),
    "payment": (0.29, 2_000, "observed", "high"),
    "event_only": (0.30, 2_050, "expected", "high"),
    "unexplained": (0.07, 450, "unexplained", "low"),
}
CANCEL_SPLIT_TOL_PTS = 0.01
# Excess cancels covering the playback + payment drivers ("nearly two-thirds": 34% + 29% = 63%).
TWO_DRIVER_SHARE = round(CANCEL_DRIVERS["playback"][0] + CANCEL_DRIVERS["payment"][0], 2)

# Day-after Sports cancels by state: playback cancels are all Dallas-Fort Worth (Texas); every other
# cancel (payment, event-only, baseline, unexplained) follows STATE_SHARE. Sums to CANCELS_OBSERVED.
CANCELS_BY_STATE = apportion(CANCELS_OBSERVED - CANCEL_DRIVERS["playback"][1], STATE_SHARE)
CANCELS_BY_STATE[METRO_STATE[INCIDENT["metro"]]] += CANCEL_DRIVERS["playback"][1]
CANCELS_BY_STATE = dict(sorted(CANCELS_BY_STATE.items(), key=lambda kv: -kv[1]))
CANCELS_BY_STATE_TOL_REL = 0.10   # per state, for states with >= 200 cancels
CANCELS_BY_STATE_TOP4 = ("Texas", "California", "Florida", "New York")
PAYMENT_PLAYBACK_OVERLAP_MAX = 0.05   # overlap assigned to payment
PLAYBACK_REBUFFER_MIN_THRESHOLD = 5.0  # minutes
PLAYBACK_SURVEY_REASON = "buffering_missed_halftime"
PLAYBACK_SURVEY_SHARE = (0.60, 0.04)   # planted vs baseline

ACQUIRER_B = {"acquirer": "B", "decline_code": "processor_timeout", "window": ("20:16", "20:40"),
              "decline_rate_baseline": 0.03, "decline_rate_incident": 0.17, "retry_scheduled": False,
              "grace_hours": 24}
ACQUIRER_DECLINE_TOL = 0.01
OTHER_ACQUIRER_DECLINE_RANGE = (0.03, 0.04)

RULED_OUT = (
    ("Price change", "The Sports price change was three weeks earlier; cancels were flat afterwards."),
    ("Other app releases", "No other app release in the window; failure rates on other versions stayed normal."),
    ("Family and Originals tiers", "Both tiers stayed inside their expected range (within 3%)."),
)

# Reconciliation (Subscriber Live vs Finance ledger)
RECON_SPORTS = {"tier": "Sports", "subscriber_live": 15_800, "ledger": 15_760, "pct_diff": 0.0025,
              "status": "within_tolerance", "likely_cause": "late-arriving events"}
RECON_SPORTS_TOL = 0.0005
RECON_ORIGINALS = {"tier": "Originals", "subscriber_live": 4_000, "ledger": 4_240, "pct_diff": 0.06,
              "status": "sources_disagree", "likely_cause": None}
RECON_ORIGINALS_TOL = 0.005
RECON_DISAGREE_THRESHOLD = 0.02

# --------------------------------------------------------------------------------------
# Transcripts and PII
# --------------------------------------------------------------------------------------
N_TRANSCRIPTS = 25_000
CONTACTS_EXCESS = 3_100
CONTACTS_EXCESS_TOL_REL = 0.05
CONTACT_EXCESS_SPLIT = {  # reason_code -> share of excess
    "stream_froze_halftime": 0.44,
    "charged_declined_cancelled": 0.35,
    "cancel_request_other": 0.21,
}
CONTACT_SPLIT_TOL_PTS = 0.02
CONTACT_EXCESS_BY_REASON = apportion(CONTACTS_EXCESS, CONTACT_EXCESS_SPLIT)  # 1,364 / 1,085 / 651
PII_SLOT_RATES = {"phone": 0.40, "email": 0.50, "card_last4": 0.30, "address": 0.20}
REDACTIONS_SEEDED = 1_600

# --------------------------------------------------------------------------------------
# Churn model (design deviation 2)
# --------------------------------------------------------------------------------------
CHURN_MULTIPLIERS = {
    "rebuffer_gt_5min": 2.3,
    "decline_no_retry": 3.5,
    "event_only_signup_30d": 2.0,
    "tenure_lt_30d": 1.6,
    "low_engagement": 1.8,
    "bundle": 0.6,
}
TRAIN_PREVALENCE = 0.05
TRAIN_COHORT = 200_000
TRAIN_SNAPSHOTS = 6
QOE_EXPOSURE_PREVALENCE = (0.18, 0.20)
AUC_CHAMPION = 0.80
AUC_CHALLENGER = 0.77
AUC_TOL = 0.01
AUC_MIN_DELTA = 0.02
SCORING_COHORT = 310_000
AT_RISK = 31_000
AT_RISK_TOL_REL = 0.02
AT_RISK_MEAN_PROB = 0.158
PROJECTED_CANCELS_30D = 4_900
PROJECTED_TOL_REL = 0.05
DRIFT_FEATURE = "rebuffer_min_30d"
DRIFT_PSI_MIN = 0.25
DRIFT_PSI_TARGET = 0.31

# --------------------------------------------------------------------------------------
# Scene 3: save plan
# --------------------------------------------------------------------------------------
ARPU_MONTHLY = 11.99
RETENTION_MONTHS = 12
ASSUMPTIONS = {
    "arpu_monthly": ARPU_MONTHLY,
    "retention_months": RETENTION_MONTHS,
    "reprocess_cost_usd": 0.40,
    "credit_redemption_rate": 0.25,
    "nudge_cost_usd": 0.05,
}

# card_id -> plan card ground truth. Revenue = saved x ARPU x retention months.
SAVE_CARDS = {
    "payment": {"title": "Fix failed payments", "n_subscribers": 5_000, "expected_cancels": 1_500,
                "save_rate": 0.50, "saved_point": 750, "saved_lo": 520, "saved_hi": 1_000,
                "offer_type": "reprocess", "cost_usd": 2_000.0},
    "playback": {"title": "Make good for buffering", "n_subscribers": 17_000, "expected_cancels": 2_380,
                 "save_rate": 0.25, "saved_point": 595, "saved_lo": 360, "saved_hi": 830,
                 "offer_type": "credit_replay", "cost_usd": 50_958.0},
    "onboarding": {"title": "Welcome event sign-ups", "n_subscribers": 9_000, "expected_cancels": 1_020,
                   "save_rate": 0.35, "saved_point": 357, "saved_lo": 220, "saved_hi": 470,
                   "offer_type": "onboarding_nudge", "cost_usd": 450.0},
}
SAVED_TOTAL = 1_700          # sum of saved_point is 1,702
SAVED_RANGE = (1_100, 2_300)
SAVED_TOL_REL = 0.05
REVENUE_PROTECTED_USD = 245_000
PLAN_COST_USD = round(sum(c["cost_usd"] for c in SAVE_CARDS.values()))  # $53,408: all three cards

# Retroactive (already cancelled): eligible counts, see design deviation 10
RETRO = {"reprocess_rate": 0.72, "reprocess_eligible": 2_000, "reprocess_saved": 1_440,
         "credit_rate": 0.24, "credit_rate_lo80": 0.16, "credit_rate_hi80": 0.32,
         "credit_eligible": 2_250, "credit_saved": 540, "total": 1_980}
RETRO_TOL_REL = 0.03
SEGMENT_GROSS = 4_700
SEGMENT_ELIGIBLE = 4_250
KIDS_ONLY_IN_SEGMENT = 160
REACTIVATED_IN_SEGMENT = 290

# Revenue at risk if no action: projected 30-day cancels (4,900, not the 31k at risk) x ARPU x 12, gross.
REVENUE_AT_RISK_USD = round(PROJECTED_CANCELS_30D * ARPU_MONTHLY * RETENTION_MONTHS)  # ~$705k

# --------------------------------------------------------------------------------------
# Scene 4: governance
# --------------------------------------------------------------------------------------
TEAM_BUDGETS = {  # team -> (monthly_budget_usd, budget_pct used, alert_pct)
    "Commerce Data": (5_000.0, 0.62, 0.90),
    "Media Engineering": (4_000.0, 0.48, 0.90),
    "Support AI": (2_500.0, 0.91, 0.90),
}
GATEWAY_CALLS_SEEDED = 172          # spec range 140-200
GATEWAY_CALLS_RANGE = (140, 200)
EVAL_V1 = {"prompt_version": "v1", "groundedness": 0.94, "correctness": 0.97, "safety": 1.0,
           "kids_pass": 1.0, "numeric_consistency": 0.98, "pii_leaks": 0, "decision": "promoted"}
EVAL_V3_BAD = {"prompt_version": "v3", "groundedness": 0.61, "correctness": 0.55, "safety": 0.90,
               "kids_pass": 0.67, "numeric_consistency": 0.48, "pii_leaks": 0, "decision": "blocked"}
PROMOTION_GATE = {"groundedness_min": 0.85, "correctness_min": 0.90, "kids_pass_min": 1.0,
                  "numeric_consistency_min": 0.95, "pii_leaks_max": 0}
INVESTIGATION_COST_USD = 0.30

# --------------------------------------------------------------------------------------
# Scope v3: governed data foundation (One Truth screen). All sources are simulated:
# files land in the raw_landing Volume tagged with the source system they stand in for.
# --------------------------------------------------------------------------------------
SOURCE_SYSTEMS = (  # source_id, display name, system type, how it would connect in production, bronze tables
    ("subscriptions", "Subscriptions & accounts", "MySQL", "Lakeflow Connect (CDC)",
     ("bronze_mysql_subscriptions", "bronze_mysql_accounts")),
    ("billing", "Billing & payment retries", "PostgreSQL", "Lakeflow Connect (CDC)",
     ("bronze_pg_billing_attempts",)),
    ("support", "Support cases & transcripts", "Salesforce Service Cloud (SaaS)", "Lakeflow Connect (SaaS connector)",
     ("bronze_sfdc_support_cases",)),
    ("payments", "Payment processor events", "Payment processor API (SaaS)", "Webhook to Zerobus Ingest",
     ("bronze_processor_events",)),
    ("telemetry", "Playback & CDN telemetry", "Event stream", "Zerobus Ingest / Kafka",
     ("bronze_session_events", "bronze_cdn_telemetry")),
)
SOURCE_IDS = tuple(s[0] for s in SOURCE_SYSTEMS)

# "Three teams, three numbers": yesterday's Sports cancels as each team's own dashboard counts them,
# all derivable from the generated data, vs the one governed Metric View definition.
TEAM_CANCEL_COUNTS = {  # team -> (count, how that team counts)
    "Product analytics": (17_100, "cancel button clicks in the app, including people who were saved or changed their mind"),
    "Finance": (RECON_SPORTS["ledger"], "billing ledger, counted when the final charge is reversed"),
    "Customer care": (12_900, "cancellations that came through a support contact"),
}
TEAM_CANCEL_TOL_REL = 0.03
GOVERNED_CANCELS = CANCELS_OBSERVED   # mv_command_center_metrics.cancels: effective cancels, net of same-day reversals
GOVERNED_DEFINITION = "Effective cancellations: subscription ended, net of reversals within 24 hours, reconciled to the billing ledger."

# --------------------------------------------------------------------------------------
# Scope v3: GenAI personalization (Save Subscribers). Three sample at-risk subscribers, one per
# plan card, each with a different story. Titles are invented; no real-world IP.
# --------------------------------------------------------------------------------------
GENRES = ("Live sports", "Sports docs", "Drama", "Comedy", "Animation", "Reality", "True crime")
SAMPLE_SUBSCRIBERS = (
    {"card_id": "playback", "first_name": "Marcus", "metro": "DFW", "tier": "Sports", "device": "Roku 7.2.0",
     "tenure_months": 26, "what_happened": "14 minutes of buffering during the halftime show",
     "fav_genre": "Live sports", "recent_titles": ("The Big Game", "Fourth Down Stories", "Sunday Rewind"),
     "watch_next": "Fourth Down Stories: Season 2", "churn_prob": 0.41},
    {"card_id": "payment", "first_name": "Priya", "metro": "PHX", "tier": "Bundle", "device": "Smart TV",
     "tenure_months": 9, "what_happened": "renewal card declined twice by a processor timeout at 8:20 PM, not retried",
     "fav_genre": "Drama", "recent_titles": ("Harbor Lights", "The Big Game", "Quiet Hours"),
     "watch_next": "Harbor Lights: Season 3", "churn_prob": 0.37},
    {"card_id": "onboarding", "first_name": "Jordan", "metro": "CHI", "tier": "Sports", "device": "iPhone",
     "tenure_months": 0, "what_happened": "signed up during the game and has watched nothing else",
     "fav_genre": "Sports docs", "recent_titles": ("The Big Game",),
     "watch_next": "Road to the Big Game", "churn_prob": 0.33},
)
PERSONALIZED_BATCH_N = AT_RISK          # ai_query writes one message per at-risk subscriber
PERSONALIZED_BATCH_COST_USD = 46.0      # gateway cost for the 31k batch, shown on the card
