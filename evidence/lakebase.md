# Lakebase: operational serving

Project `streamco`, branch `production`, database `databricks_postgres`. Synced tables (snapshot) in Postgres schema `streamco`; the app's approvals in `streamco_app`.

## Synced tables

| synced table | source | state |
|---|---|---|
| cindy_wu_catalog.streamco.churn_scores_serving |  | SyncedTableState.SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE |
| cindy_wu_catalog.streamco.personalized_messages_serving |  | SyncedTableState.SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE |

## Row counts: Delta source vs Lakebase

| table | delta_rows | lakebase_rows | match |
|---|---|---|---|
| churn_scores | 299624 | 299624 | True |
| personalized_messages | 28269 | 28269 | True |

## One-subscriber lookups (the app's read)

```sql
SELECT s.churn_prob, s.risk_decile, s.card_id, m.subject FROM streamco.churn_scores_serving s LEFT JOIN streamco.personalized_messages_serving m USING (subscriber_id) WHERE s.subscriber_id = %(id)s
```

| subscriber | id | churn_prob | decile | card | message subject | query ms |
|---|---|---|---|---|---|---|
| Marcus | 643364 | 0.418 | 10 | playback | We're sorry about the halftime buffering | 238.9 |
| Priya | 1611257 | 0.373 | 10 | payment | We’re sorry we didn’t retry your renewal | 167.2 |
| Jordan | 3877967 | 0.606 | 10 | onboarding | Welcome after The Big Game, Jordan | 172.3 |

## Read access for the app's role

| grantee | table_schema | table_name | privilege_type |
|---|---|---|---|
| databricks_superuser | streamco | churn_scores_serving | INSERT |
| databricks_superuser | streamco | churn_scores_serving | SELECT |
| databricks_superuser | streamco | churn_scores_serving | UPDATE |
| databricks_superuser | streamco | churn_scores_serving | DELETE |
| databricks_superuser | streamco | churn_scores_serving | TRUNCATE |
| databricks_superuser | streamco | churn_scores_serving | REFERENCES |
| databricks_superuser | streamco | churn_scores_serving | TRIGGER |
| <app-service-principal-id> | streamco | churn_scores_serving | SELECT |
| databricks_superuser | streamco | personalized_messages_serving | INSERT |
| databricks_superuser | streamco | personalized_messages_serving | SELECT |
| databricks_superuser | streamco | personalized_messages_serving | UPDATE |
| databricks_superuser | streamco | personalized_messages_serving | DELETE |
| databricks_superuser | streamco | personalized_messages_serving | TRUNCATE |
| databricks_superuser | streamco | personalized_messages_serving | REFERENCES |
| databricks_superuser | streamco | personalized_messages_serving | TRIGGER |
| <app-service-principal-id> | streamco | personalized_messages_serving | SELECT |

## Approvals written by the deployed app

_None yet (UndefinedTable)._
