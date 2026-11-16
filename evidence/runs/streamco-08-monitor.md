# streamco-08-monitor

- Run id: 990068716512549
- Result: SUCCESS
- Started: 2026-11-09 18:07 UTC
- Duration: 214 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/08_monitor`  
Task run id: 570553139565629

#### Cell 1: `# StreamCo Command Center 08: churn model monitoring, the retrain rule, and the outcomes loop.`

_(no output)_

#### Cell 2: `%pip install -q lightgbm>=4.5 mlflow>=3.4`

```
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed. This behaviour is the source of the following dependency conflicts.
jupyter-server 1.23.4 requires anyio<4,>=3.1.0, but you have anyio 4.16.0 which is incompatible.
googleapis-common-protos 1.62.0 requires protobuf!=3.20.0,!=3.20.1,!=4.21.1,!=4.21.2,!=4.21.3,!=4.21.4,!=4.21.5,<5.0.0.dev0,>=3.19.5, but you have protobuf 7.36.2 which is incompatible.
Note: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.
```

#### Cell 3: `dbutils.library.restartPython()  # noqa: F821`

```
Warning: statements after `dbutils.library.restartPython()` will execute before Python is restarted.
```

#### Cell 4: `import datetime as dt`

_(no output)_

#### Cell 5: `# 1. Champion`

```
cindy_wu_catalog.streamco.churn_propensity v1 @champion, trained 2026-11-09 04:40 UTC; other versions: none
```

#### Cell 6: `# 2. Performance on each labelled window (held-out subscribers only)`

```
AUC at training 0.8033; latest labelled window 2027-01-15 (n=40,057) 0.8085; drop -0.0051
by window: [('2026-08-18', 0.8046), ('2026-09-17', 0.7916), ('2026-10-17', 0.8088), ('2026-11-16', 0.8063), ('2026-12-16', 0.8013), ('2027-01-15', 0.8085)]
```

#### Cell 7: `# 3. Drift: PSI per feature, training snapshots -> post-game scoring snapshot`

```
feature  importance_rank       psi  baseline_mean  current_mean     drift
0    days_since_last_session                1  1.279918       3.820578      1.089969      high
1                 plan_price               14  0.671582      12.244838     11.924077      high
2                tenure_days                7  0.529398     487.403709    322.975456      high
3            active_days_30d               12  0.511668      17.657776     12.313530      high
4                  hours_30d                4  0.487529      15.574493     11.679937      high
5              signup_source               13  0.189834            NaN           NaN  moderate
6          event_only_signup               11  0.185257       0.141865      0.319414  moderate
7                  is_bundle                8  0.014346       0.079654      0.050363       low
8           rebuffer_min_30d                6  0.012215       5.822795      6.183021       low
9   max_session_rebuffer_min                2  0.009753       2.772088      2.912050       low
10         failed_starts_30d               15  0.003083       0.992132      1.064034       low
11              declines_30d               16  0.001361       0.107896      0.120511       low
12          engagement_trend                3  0.000040       0.000136     -0.000308       low
13      support_contacts_30d                9  0.000008       0.279027      0.280151       low
14                pauses_90d               10  0.000005       0.234062      0.235068       low
15          decline_no_retry                5  0.000003       0.040339      0.040003       low
model score PSI: 0.0657
```

#### Cell 8: `# 4. Status by the retrain rule`

```
RETRAIN - PSI above 0.2 on days since last watched (1.28), low viewing hours (0.49)
```

#### Cell 9: `# 5. Outcomes loop: the holdout tests behind each card's save rate (same filter and formula as 03)`

```
tests  from_tests      plan
card_id                                
onboarding      6    0.349979  0.349979
payment         6    0.499989  0.499989
playback        6    0.249953  0.249953
```

#### Cell 10: `# Write the three tables`

_(no output)_

#### Cell 11: `# Validation (structure only; the numbers are reported as found)`

```
{"model_name": "cindy_wu_catalog.streamco.churn_propensity", "champion_version": "1", "trained_at": "2026-11-09 04:40:30.890000+00:00", "auc_train": 0.8033373249450113, "auc_recent": 0.8084777099411902, "recent_window": "2027-01-15", "recent_window_n": 40057, "auc_drop": -0.0051403849961788595, "scoring_snapshot": "2027-02-15", "n_scored": 299624, "score_psi": 0.0656856171009951, "top_features": ["Days since last watched", "Worst buffering in one session", "Watching less", "Low viewing hours", "Failed payment, never retried"], "max_top_psi": 1.2799178236630744, "max_top_psi_feature": "Days since last watched", "n_top_features_drifted": 2, "status": "retrain", "status_reason": "PSI above 0.2 on days since last watched (1.28), low viewing hours (0.49)", "retrain_rule": "Retrain when PSI > 0.2 on a top-5 feature or AUC drops more than 0.03 from training; a challenger replaces the champion only if it beats it on the same held-out window.", "psi_retrain": 0.2, "auc_drop_retrain": 0.03, "challenger_version": null, "monitored_at": "2026-11-09 18:10:14.828978+00:00", "psi": {"days_since_last_session": 1.2799, "plan_price": 0.6716, "tenure_days": 0.5294, "active_days_30d": 0.5117, "hours_30d": 0.4875, "signup_source": 0.1898, "event_only_signup": 0.1853, "is_bundle": 0.0143, "rebuffer_min_30d": 0.0122, "max_session_rebuffer_min": 0.0098, "failed_starts_30d": 0.0031, "declines_30d": 0.0014, "engagement_trend": 0.0, "support_contacts_30d": 0.0, "pauses_90d": 0.0, "decline_no_retry": 0.0}, "outcomes": {"onboarding": {"tests": 6, "from_tests": 0.35, "plan": 0.35}, "payment": {"tests": 6, "from_tests": 0.5, "plan": 0.5}, "playback": {"tests": 6, "from_tests": 0.25, "plan": 0.25}}}
```
