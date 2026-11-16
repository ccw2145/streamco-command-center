# Genie Agent: natural-language questions (live)

Genie Agent "StreamCo Subscriber Insights" (`01f1dbf923701cb3b3ec3d2fcbee9e87`), asked when this file was generated.

### Q: How many Sports cancellations were there on 2027-02-15, and how many were expected?

Status: COMPLETED

```sql
WITH `actuals` AS (
  SELECT
    `day`,
    `tier`,
    MEASURE(`cancels`) AS `actual_cancels`
  FROM `cindy_wu_catalog`.`streamco`.`mv_streamco_metrics`
  WHERE `day` = DATE'2027-02-15'
    AND `tier` = 'Sports'
    AND `tier` IS NOT NULL
  GROUP BY ALL
), `recon` AS (
  SELECT
    `day`,
    `tier`,
    `subscriber_live_cancels`,
    `ledger_cancels`,
    `pct_diff`,
    `status`
  FROM `cindy_wu_catalog`.`streamco`.`gold_reconciliation_daily`
  WHERE `day` = DATE'2027-02-15'
    AND `tier` = 'Sports'
), `expected` AS (
  SELECT
    `day`,
    `tier`,
    `yhat` AS `expected_cancels`,
    `lo` AS `normal_lo`,
    `hi` AS `normal_hi`,
    `outside_band`
  FROM `cindy_wu_catalog`.`streamco`.`gold_cancel_expected`
  WHERE `day` = DATE'2027-02-15'
    AND `tier` = 'Sports'
)
SELECT
  a.`day`,
  a.`tier`,
  a.`actual_cancels`,
  e.`expected_cancels`,
  e.`normal_lo`,
  e.`normal_hi`,
  e.`outside_band`,
  r.`subscriber_live_cancels`,
  r.`ledger_cancels`,
  r.`pct_diff`,
  r.`status`
FROM `actuals` a
LEFT JOIN `expected` e
  ON a.`day` = e.`day` AND a.`tier` = e.`tier`
LEFT JOIN `recon` r
  ON a.`day` = r.`day` AND a.`tier` = r.`tier`
```

| day | tier | actual_cancels | expected_cancels | normal_lo | normal_hi | outside_band | subscriber_live_cancels | ledger_cancels | pct_diff | status |
|---|---|---|---|---|---|---|---|---|---|---|
| 2027-02-15 | Sports | 15800 | 8977.166666666666 | 8327.297545227588 | 9627.035788105744 | true | 15800 | 15760 | 0.002531645569620253 | within_tolerance |

On **2027-02-15**, the **Sports** tier had **15,800** cancellations, and the expected level was **8,977.2**; the reconciliation status for **Sports** on **2027-02-15** is **within_tolerance**. This was **above normal**, since the expected range was **8,327.3 to 9,627.0** and `outside_band` is **true**.

### Q: Show Sports cancellations by state on 2027-02-15, top 10.

Status: COMPLETED

```sql
SELECT
  `state`,
  MEASURE(`cancels`) AS `cancels`
FROM `cindy_wu_catalog`.`streamco`.`mv_streamco_metrics`
WHERE `day` = DATE '2027-02-15'
  AND `tier` = 'Sports'
  AND `state` IS NOT NULL
GROUP BY ALL
ORDER BY `cancels` DESC, `state` ASC
LIMIT 10
```

| state | cancels |
|---|---|
| Texas | 3488 |
| California | 1606 |
| Florida | 894 |
| New York | 761 |
| Pennsylvania | 543 |
| Illinois | 509 |
| Georgia | 467 |
| North Carolina | 460 |
| Ohio | 447 |
| Michigan | 406 |

Examples include:
- **Texas**: **3,488** cancellations
- **California**: **1,606** cancellations
- **Florida**: **894** cancellations
- **New York**: **761** cancellations
- **Pennsylvania**: **543** cancellations

Across the 10 states shown, **Texas** is the clear leader, with more than double **California** and over eight times **Michigan**, the lowest state in this top-10 list.

### Q: Did Originals cancellations go up too?

Status: COMPLETED

```sql
WITH `orig_metrics` AS (
  SELECT
    `day`,
    `tier`,
    MEASURE(`cancels`) AS `cancels`
  FROM `cindy_wu_catalog`.`streamco`.`mv_streamco_metrics`
  WHERE `day` = DATE'2027-02-15'
    AND `tier` = 'Originals'
    AND `tier` IS NOT NULL
    AND `day` IS NOT NULL
  GROUP BY ALL
)
SELECT
  r.`day`,
  r.`tier`,
  m.`cancels`,
  e.`yhat` AS `expected_cancels`,
  e.`lo` AS `normal_lo`,
  e.`hi` AS `normal_hi`,
  e.`outside_band`,
  r.`subscriber_live_cancels`,
  r.`ledger_cancels`,
  r.`pct_diff`,
  r.`status`
FROM `cindy_wu_catalog`.`streamco`.`gold_reconciliation_daily` r
LEFT JOIN `orig_metrics` m
  ON r.`day` = m.`day` AND r.`tier` = m.`tier`
LEFT JOIN `cindy_wu_catalog`.`streamco`.`gold_cancel_expected` e
  ON r.`day` = e.`day` AND r.`tier` = e.`tier`
WHERE r.`day` = DATE'2027-02-15'
  AND r.`tier` = 'Originals'
```

| day | tier | cancels | expected_cancels | normal_lo | normal_hi | outside_band | subscriber_live_cancels | ledger_cancels | pct_diff | status |
|---|---|---|---|---|---|---|---|---|---|---|
| 2027-02-15 | Originals | 4017 | 4000.0 | 3699.787075561361 | 4300.212924438639 | false | 4017 | 4268 | 0.062484441125217824 | sources_disagree |

I can’t give a confident Originals cancellation count for **2027-02-15** because the reconciliation status for **Originals** is **sources_disagree**. For **Originals** on **2027-02-15**, Subscriber Live shows **4,017** cancellations, the Finance ledger shows **4,268** cancellations, and the gap is **251** cancellations (**6.2%**); based on the expected range, Subscriber Live’s **4,017** is within the normal band of **3,699.8 to 4,300.2** around an expected **4,000.0**.
