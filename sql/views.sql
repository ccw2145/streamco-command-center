-- StreamCo demo-layer views. Run by notebooks/01_setup.py. The generators (data/) already write StreamCo tier
-- names into {catalog}.{src}, so a view exists here only when it adds something: masking, the kids filter,
-- the label join for training, or the eligible scoring population.
-- Tokens: {catalog}, {schema}, {src} (= {schema} for StreamCo), {day_after}, {feature_cols_f}.
-- Statements end with ';' at the end of a line; keep ';' off line ends inside a statement.

CREATE OR REPLACE VIEW {catalog}.{schema}.v_subscribers
COMMENT 'Subscribers. Email, phone and address are masked unless the caller is in cc_privileged. Kids-only accounts are filtered out.'
AS SELECT subscriber_id, account_id, tier, is_bundle, signup_ts, signup_source, plan_price, metro, state,
       device_primary, acquirer, status, canceled_ts,
       CASE WHEN is_account_group_member('cc_privileged') THEN email
            ELSE regexp_replace(email, '^(.)[^@]*@', '$1***@') END AS email,
       CASE WHEN is_account_group_member('cc_privileged') THEN phone
            ELSE concat('***-***-', right(phone, 4)) END AS phone,
       CASE WHEN is_account_group_member('cc_privileged') OR postal_address IS NULL THEN postal_address
            ELSE '[ADDRESS]' END AS postal_address
FROM {catalog}.{src}.dim_subscribers
WHERE NOT coalesce(is_kids_only_account, false);

CREATE OR REPLACE VIEW {catalog}.{schema}.v_support_showcase
COMMENT 'The showcase support chat for the governance drawer. masked_text is redacted for everyone; transcript_text is clear only for cc_privileged members.'
AS SELECT contact_id, contact_ts, channel,
       {catalog}.{src}.fn_redact_pii(transcript_text) AS masked_text,
       CASE WHEN is_account_group_member('cc_privileged') THEN transcript_text
            ELSE {catalog}.{src}.fn_redact_pii(transcript_text) END AS transcript_text,
       {catalog}.{src}.fn_count_redactions({catalog}.{src}.fn_redact_pii(transcript_text)) AS n_redactions,
       is_account_group_member('cc_privileged') AS viewer_is_privileged
FROM {catalog}.{src}.fact_support_contacts
WHERE is_showcase;

CREATE OR REPLACE VIEW {catalog}.{schema}.v_training_features
COMMENT 'Churn training set: historical monthly feature snapshots joined to the 30-day churn label.'
AS SELECT {feature_cols_f}, CAST(l.churned_30d AS INT) AS churned_30d
FROM {catalog}.{src}.gen_subscriber_features f
JOIN {catalog}.{src}.labels_churn_30d l ON l.subscriber_id = f.subscriber_id AND l.snapshot_date = f.snapshot_date
WHERE f.snapshot_date < DATE'{day_after}';

CREATE OR REPLACE VIEW {catalog}.{schema}.v_scoring_features
COMMENT 'Eligible post-game population for churn scoring: active, not kids-only, never cancelled (canceled_ts IS NULL).'
AS SELECT d.account_id, d.tier, {feature_cols_f}
FROM {catalog}.{src}.gen_subscriber_features f
JOIN {catalog}.{src}.dim_subscribers d ON d.subscriber_id = f.subscriber_id
WHERE f.snapshot_date = DATE'{day_after}'
  AND d.status = 'active' AND NOT coalesce(d.is_kids_only_account, false) AND d.canceled_ts IS NULL;

-- The Ask agent's UC function tool.
CREATE OR REPLACE FUNCTION {catalog}.{schema}.fn_cancel_drivers(
  day DATE COMMENT 'Day of the cancellations, e.g. the day after the Big Game',
  tier STRING COMMENT 'Tier: Sports, Family, Originals or Bundle')
RETURNS TABLE (driver STRING, n_cancels INT, pct DOUBLE, evidence_type STRING, confidence STRING, evidence STRING,
               observed INT, expected INT, recon_status STRING, recon_pct_diff DOUBLE, ledger_cancels INT)
COMMENT 'Why cancellations moved for one tier on one day. One row per driver (playback = buffering, payment = declined and never retried, event_only = expected event sign-up cancels, unexplained, baseline) with its count, share of the excess above normal (pct, a fraction; null for baseline), evidence type, confidence and the evidence rule. Every row also carries the tier''s observed and expected totals and the reconciliation with the Finance ledger. If recon_status is sources_disagree, do not state a cancellation number as fact. If driver is null, no decomposition exists for that tier and day.'
RETURN
  SELECT d.driver, d.n_cancels, d.pct, d.evidence_type, d.confidence, d.rule AS evidence,
         r.subscriber_live_cancels AS observed, CAST(round(e.yhat) AS INT) AS expected,
         r.status AS recon_status, r.pct_diff AS recon_pct_diff, r.ledger_cancels
  FROM {catalog}.{src}.gold_reconciliation_daily r
  LEFT JOIN {catalog}.{src}.gold_cancel_expected e ON e.day = r.day AND e.tier = r.tier
  LEFT JOIN {catalog}.{src}.gold_cancel_decomposition d ON d.day = r.day AND d.tier = r.tier
  WHERE r.day = fn_cancel_drivers.day AND r.tier = fn_cancel_drivers.tier
  ORDER BY CASE d.driver WHEN 'playback' THEN 0 WHEN 'payment' THEN 1 WHEN 'event_only' THEN 2
                         WHEN 'unexplained' THEN 3 ELSE 4 END;
