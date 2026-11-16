-- Owner: Stream A (Data + Pipelines). cc_batch MV: governed (Subscriber Live) cancels vs the Finance ledger per ET day and tier.
-- status: ok below 0.1%, within_tolerance up to the threshold, sources_disagree above it. The threshold is
-- planted.RECON_DISAGREE_THRESHOLD, read from ref_thresholds (written by 01_setup_uc).
CREATE OR REFRESH MATERIALIZED VIEW gold_reconciliation_daily
COMMENT 'Daily effective cancel counts vs the Finance ledger. status sources_disagree means the two systems differ by more than the reconciliation threshold and no confident number should be quoted.'
AS WITH live AS (
  SELECT day, tier, count(*) AS n FROM gold_cancel_events GROUP BY ALL
), thr AS (
  SELECT max(value) AS t FROM ref_thresholds WHERE key = 'recon_disagree_threshold'
)
SELECT l.day, l.tier,
       CAST(l.n AS INT) AS subscriber_live_cancels,
       CAST(g.ledger_cancels AS INT) AS ledger_cancels,
       abs(g.ledger_cancels - l.n) / l.n AS pct_diff,
       CASE WHEN g.ledger_cancels IS NULL THEN NULL
            WHEN abs(g.ledger_cancels - l.n) / l.n > thr.t THEN 'sources_disagree'
            WHEN abs(g.ledger_cancels - l.n) / l.n >= 0.001 THEN 'within_tolerance'
            ELSE 'ok' END AS status,
       CASE WHEN g.ledger_cancels < l.n AND abs(g.ledger_cancels - l.n) / l.n BETWEEN 0.001 AND thr.t
            THEN 'late-arriving events' END AS likely_cause
FROM live l CROSS JOIN thr
LEFT JOIN billing_ledger_cancels_daily g ON g.day = l.day AND g.tier = l.tier;
