# Predictive ML: forecast and churn

## Unity Catalog model registry

| model | version | aliases | created | status |
|---|---|---|---|---|
| cindy_wu_catalog.streamco.viewer_forecast | 1 |  | 2026-11-09 04:30 UTC | ModelVersionInfoStatus.READY |
| cindy_wu_catalog.streamco.churn_propensity | 1 |  | 2026-11-09 04:40 UTC | ModelVersionInfoStatus.READY |

### Forecast: capacity flags before kickoff

```sql
SELECT * FROM cindy_wu_catalog.streamco.forecast_flags ORDER BY util_at_peak DESC LIMIT 8
```

| metro | cdn | util_at_peak | flagged |
|---|---|---|---|
| DFW | CDN-C | 0.968 | true |
| DFW | CDN-B | 0.8012 | false |
| HOU | CDN-C | 0.7904 | false |
| PHL | CDN-B | 0.787 | false |
| DEN | CDN-G | 0.7834 | false |
| MSP | CDN-A | 0.7791 | false |
| LAX | CDN-D | 0.777 | false |
| SEA | CDN-F | 0.7736 | false |

### Churn model card

```sql
SELECT * FROM cindy_wu_catalog.streamco.churn_model_card
```

| model_name | version | auc | n_scored | n_at_risk | projected_cancels_30d | top_drivers |
|---|---|---|---|---|---|---|
| cindy_wu_catalog.streamco.churn_propensity | 1 | 0.8033373249450113 | 299624 | 29963 | 6696.375422248191 | ["Days since last watched","Worst buffering in one session","Watching less","Low viewing hours","Failed payment, never retried"] |

### Save plan

```sql
SELECT card_id, title, n_subscribers, expected_cancels, save_rate, saved_point, saved_lo, saved_hi, cost_usd, revenue_protected_usd FROM cindy_wu_catalog.streamco.save_plan
```

| card_id | title | n_subscribers | expected_cancels | save_rate | saved_point | saved_lo | saved_hi | cost_usd | revenue_protected_usd |
|---|---|---|---|---|---|---|---|---|---|
| payment | Fix failed payments | 4556 | 1247.2996386961347 | 0.4999890224004844 | 623.6361269921578 | 406.8104461174509 | 840.8802933594828 | 1822.4 | 89728.76595163166 |
| playback | Make good for buffering | 13165 | 3216.786782229499 | 0.24995321002954352 | 804.0461821988694 | 459.1228329483148 | 1144.100954836738 | 39462.0875 | 115686.16469477332 |
| onboarding | Welcome event sign-ups | 10548 | 1921.0676015407148 | 0.3499786641674012 | 672.3326729624928 | 417.38787439149263 | 926.6553827758158 | 527.4 | 96735.22498584347 |

### Monitoring: accuracy and drift status

```sql
SELECT champion_version, auc_train, auc_recent, recent_window, auc_drop, max_top_psi, max_top_psi_feature, status, status_reason, retrain_rule FROM cindy_wu_catalog.streamco.churn_monitoring
```

| champion_version | auc_train | auc_recent | recent_window | auc_drop | max_top_psi | max_top_psi_feature | status | status_reason | retrain_rule |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.8033373249450113 | 0.8084777099411902 | 2027-01-15 | -0.0051403849961788595 | 1.2799178236630744 | Days since last watched | retrain | PSI above 0.2 on days since last watched (1.28), low viewing hours (0.49) | Retrain when PSI > 0.2 on a top-5 feature or AUC drops more than 0.03 from training; a challenger replaces the champion only if it beats it on the same held-out |

### Monitoring: drift per feature (PSI)

```sql
SELECT label, importance_rank, is_top, psi, baseline_mean, current_mean, drift FROM cindy_wu_catalog.streamco.churn_monitoring_features ORDER BY psi DESC
```

| label | importance_rank | is_top | psi | baseline_mean | current_mean | drift |
|---|---|---|---|---|---|---|
| Days since last watched | 1 | true | 1.2799178236630744 | 3.820578333333333 | 1.0899694283501988 | high |
| Plan price | 14 | false | 0.6715824800474697 | 12.244837500000008 | 11.924077376979154 | high |
| New subscriber | 7 | false | 0.5293978224926063 | 487.40370916666666 | 322.9754559047339 | high |
| Few active days | 12 | false | 0.5116679175037555 | 17.657775833333332 | 12.313529623795157 | high |
| Low viewing hours | 4 | true | 0.48752887627131136 | 15.57449335833334 | 11.679937321442871 | high |
| How they signed up | 13 | false | 0.1898342570705734 |  |  | moderate |
| Joined for the event | 11 | false | 0.18525744881611195 | 0.141865 | 0.3194136651269591 | moderate |
| Not on a bundle | 8 | false | 0.014345738075983754 | 0.07965416666666666 | 0.05036312177929672 | low |
| Buffering minutes | 6 | false | 0.01221531725816864 | 5.8227946166666635 | 6.18302095292767 | low |
| Worst buffering in one session | 2 | true | 0.009753372918967097 | 2.7720876916666666 | 2.9120499692948503 | low |
| Streams that failed to start | 15 | false | 0.003083367916671423 | 0.9921316666666666 | 1.0640335887645849 | low |
| Card declines | 16 | false | 0.0013610499031296304 | 0.10789583333333333 | 0.12051104050409847 | low |
| Watching less | 3 | true | 3.990970098115447E-5 | 1.3580591666666647E-4 | -3.080324006087633E-4 | low |
| Contacted support | 9 | false | 8.314783913591067E-6 | 0.27902666666666665 | 0.2801511227405014 | low |
| Paused recently | 10 | false | 5.151373904004209E-6 | 0.23406166666666667 | 0.23506795183296397 | low |
| Failed payment, never retried | 5 | true | 2.922698651218242E-6 | 0.04033916666666667 | 0.040003471017007984 | low |

### Learning from outcomes: holdout tests behind each save rate

```sql
SELECT card_id, test_id, run_date, n_treated, n_holdout, treated_rate, holdout_rate, lift, plan_save_rate FROM cindy_wu_catalog.streamco.churn_monitoring_outcomes ORDER BY card_id, run_date
```

| card_id | test_id | run_date | n_treated | n_holdout | treated_rate | holdout_rate | lift | plan_save_rate |
|---|---|---|---|---|---|---|---|---|
| onboarding | HT-ONBO-PRE-1 | 2026-02-23 | 3355 | 3901 | 0.24530551415797316 | 0.02999230966418867 | 0.2153132044937845 | 0.3499786641674012 |
| onboarding | HT-ONBO-PRE-5 | 2026-05-06 | 5844 | 3600 | 0.5147159479808351 | 0.03 | 0.48471594798083506 | 0.3499786641674012 |
| onboarding | HT-ONBO-PRE-2 | 2026-05-14 | 4215 | 3040 | 0.3594306049822064 | 0.02993421052631579 | 0.32949639445589063 | 0.3499786641674012 |
| onboarding | HT-ONBO-PRE-3 | 2026-06-22 | 3458 | 3697 | 0.4456333140543667 | 0.030024344062753585 | 0.4156089699916131 | 0.3499786641674012 |
| onboarding | HT-ONBO-PRE-4 | 2026-06-24 | 4659 | 3164 | 0.40051513200257566 | 0.03002528445006321 | 0.37048984755251246 | 0.3499786641674012 |
| onboarding | HT-ONBO-PRE-6 | 2026-06-28 | 3571 | 4207 | 0.31419770372444694 | 0.029950083194675542 | 0.28424762052977137 | 0.3499786641674012 |
| payment | HT-REPR-PRE-2 | 2026-03-08 | 4624 | 3764 | 0.35423875432525953 | 0.03002125398512221 | 0.3242175003401373 | 0.4999890224004844 |
| payment | HT-REPR-PRE-4 | 2026-04-03 | 5536 | 4305 | 0.6157875722543352 | 0.029965156794425088 | 0.5858224154599101 | 0.4999890224004844 |
| payment | HT-REPR-PRE-1 | 2026-04-30 | 4571 | 5625 | 0.5567709472763072 | 0.030044444444444443 | 0.5267265028318627 | 0.4999890224004844 |
| payment | HT-REPR-PRE-6 | 2026-05-23 | 5639 | 5718 | 0.7057989005142756 | 0.030080447708989155 | 0.6757184528052864 | 0.4999890224004844 |
| payment | HT-REPR-PRE-3 | 2026-06-20 | 3460 | 4548 | 0.44421965317919077 | 0.02990325417766051 | 0.4143163990015303 | 0.4999890224004844 |
| payment | HT-REPR-PRE-5 | 2026-08-15 | 5141 | 5220 | 0.5032094923166699 | 0.03007662835249042 | 0.4731328639641795 | 0.4999890224004844 |
| playback | HT-CRED-PRE-6 | 2026-05-06 | 4057 | 4093 | 0.2965245255114617 | 0.03005130710969949 | 0.2664732184017622 | 0.24995321002954352 |
| playback | HT-CRED-PRE-3 | 2026-05-09 | 3444 | 5400 | 0.227061556329849 | 0.03 | 0.197061556329849 | 0.24995321002954352 |
| playback | HT-CRED-PRE-5 | 2026-05-13 | 4321 | 4854 | 0.33279333487618606 | 0.030078285949732178 | 0.3027150489264539 | 0.24995321002954352 |
| playback | HT-CRED-PRE-1 | 2026-06-12 | 3526 | 3296 | 0.3885422575155984 | 0.030036407766990292 | 0.3585058497486081 | 0.24995321002954352 |
| playback | HT-CRED-PRE-2 | 2026-06-24 | 4451 | 5806 | 0.17142215232532015 | 0.029968997588701342 | 0.14145315473661882 | 0.24995321002954352 |
| playback | HT-CRED-PRE-4 | 2026-12-11 | 4563 | 4814 | 0.2634231865001096 | 0.029912754466140425 | 0.23351043203396918 | 0.24995321002954352 |
