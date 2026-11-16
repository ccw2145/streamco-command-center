# Personalized messages (ai_query batch)

### The batch run

```sql
SELECT * FROM cindy_wu_catalog.streamco.personalization_run ORDER BY run_at DESC
```

| n_messages | cost_usd | model | run_at |
|---|---|---|---|
| 28269 | 57.58 | system.ai.gpt-5-6-sol | 2026-11-09T21:08:46.356Z |

### Messages for the three sample subscribers

```sql
SELECT s.first_name, m.card_id, m.subject, m.body, m.watch_next, m.agent_note FROM cindy_wu_catalog.streamco.personalized_messages m JOIN cindy_wu_catalog.streamco.ref_sample_subscribers s USING (subscriber_id)
```

| first_name | card_id | subject | body | watch_next | agent_note |
|---|---|---|---|---|---|
| Marcus | playback | We're sorry about the halftime buffering | Hi Marcus, you had 14 minutes of buffering during the halftime show, and StreamCo let you down. We'd like to make it right with one month of service credit and  | Fourth Down Stories: Season 2 | Marcus experienced 14 minutes of buffering during the halftime show. Offer one month of service credit and a replay of the missed moments. Suggest Fourth Down S |
| Priya | payment | We’re sorry we didn’t retry your renewal | Hi Priya, your renewal card was declined twice after a processor timeout at 8:20 PM, and we didn’t retry it. We can retry the failed payment at no charge so you | Harbor Lights: Season 3 | The renewal card was declined twice after a processor timeout and wasn’t retried. Offer to retry the failed payment at no charge so the account stays active. Su |
| Jordan | onboarding | Welcome after The Big Game, Jordan | Hi Jordan, you signed up during The Big Game and haven’t watched anything else yet. Welcome to StreamCo—we’re glad you’re here. Since you enjoy sports docs, Roa | Road to the Big Game | Jordan signed up during The Big Game and hasn’t watched anything else. Offer a warm welcome with a next-watch suggestion only. Recommend Road to the Big Game. |

### Offer-policy passage retrieved with AI Search for each plan card

```sql
SELECT card_id, doc_title, score, passage FROM cindy_wu_catalog.streamco.knowledge_citations
```

| card_id | doc_title | score | passage |
|---|---|---|---|
| payment | Save-offer policy | 0.66477805 | 1. Payment failures: Involuntary (payment) cancellations receive a free payment retry. No discount or credit is offered. |
| playback | Save-offer policy | 0.67771834 | 2. Service failures: Subscribers with a verified service failure may receive one one-month credit per 12 months. |
| onboarding | Save-offer policy | 0.7111078 | 3. New event sign-ups: Event sign-ups in their first 30 days receive onboarding messages only. No discount is offered. |

### Knowledge index source: policy sections and masked transcripts

```sql
SELECT doc_type, count(*) AS chunks FROM cindy_wu_catalog.streamco.knowledge_chunks GROUP BY 1
```

| doc_type | chunks |
|---|---|
| transcript | 20621 |
| policy | 13 |
