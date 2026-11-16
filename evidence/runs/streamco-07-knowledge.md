# streamco-07-knowledge

- Run id: 315988607165488
- Result: SUCCESS
- Started: 2026-11-09 18:58 UTC
- Duration: 650 s

## Task `t` (SUCCESS)

Notebook: `/Workspace/Users/<user>/streamco-build/notebooks/07_knowledge`  
Task run id: 1087967114108939

#### Cell 1: `# StreamCo Command Center 07: the knowledge index the GenAI layer retrieves and cites.`

_(no output)_

#### Cell 2: `import datetime as dt`

_(no output)_

#### Cell 3: `# Policy docs: one chunk per "## n. Section". The HTML owner comment and "(synthetic)" are dropped.`

```
policy chunks: 13 from 4 docs
```

#### Cell 4: `# Support transcripts: masked text only, kids-only accounts out (v_subscribers' row filter, plus an explicit check).`

```
{'emails': 0, 'phones': 0, 'cards': 0, 'brands': 0, 'n': 20634}
```

#### Cell 5: `# knowledge_chunks: MERGE (not overwrite) so the Delta Sync index sees row-level changes through the change feed.`

```
staged 20,634 chunks in 0.2 min
knowledge_chunks: {'transcript': 20621, 'policy': 13}
```

#### Cell 6: `# The index: Delta Sync, managed embeddings, on the existing endpoint. Created once; later runs trigger a sync.`

```
sync triggered for cindy_wu_catalog.streamco.knowledge_idx
19:07:52 ready=True indexed=20,634/20,634 sync=CREATED
19:08:23 ready=True indexed=20,634/20,634 sync=RUNNING
19:08:54 ready=True indexed=20,634/20,634 sync=COMPLETED
```

#### Cell 7: `# Retrieval checks, then the offer-policy passage for each save-plan card.`

```
sign-up payment declined processor timeout retry grace window
  0.760 Payment retry policy / 3. Change proposed after the Big Game: Add processor_timeout to the sign-up retry list, with one retry after 15 minutes through a different acquirer.
  0.696 Payment retry policy / 1. Retry schedule: Declined renewals are retried after 1, 3 and 7 days. Declined sign-ups are retried once after 1 hour.
  0.694 Payment retry policy / 2. Decline codes without a retry: The decline code processor_timeout is not on the retry list. Accounts declined with this code are not retried 

I was charged, my card was declined and my account was cancelled
  0.644 Support email transcript / charged, card declined, account cancelled: Agent: Hello, and thanks for your email. Let me look into this for you.
Customer: I signed up during the game,
  0.642 Support chat transcript / charged, card declined, account cancelled: Agent: Thanks for reaching out on chat. What's going on with your account?
Customer: The payment never went th
  0.640 Support chat transcript / charged, card declined, account cancelled: Agent: Hello! You're chatting with the support team. How can I help?
Customer: The payment never went through 

stream froze during halftime
  0.683 Support phone transcript / stream froze at halftime: Agent: Thanks for calling support, how can I help you today?
Customer: I paid for the game and the stream lock
  0.678 Support chat transcript / stream froze at halftime: Agent: Thanks for reaching out on chat. What's going on with your account?
Customer: Halftime show would not p
  0.678 Support chat transcript / stream froze at halftime: Agent: Hello! You're chatting with the support team. How can I help?
Customer: The stream kept freezing right 
payment     0.665 Save-offer policy / 1. Payment failures: Involuntary (payment) cancellations receive a free payment retry. No discount or credit is offered.
playback    0.678 Save-offer policy / 2. Service failures: Subscribers with a verified service failure may receive one one-month credit per 12 months.
onboarding  0.711 Save-offer policy / 3. New event sign-ups: Event sign-ups in their first 30 days receive onboarding messages only. No discount is offered.
DataFrame[]
```

#### Cell 8: `summary = {"index": INDEX, "endpoint": W["endpoint"], "embedding_model": W["embedding_model"], "chunks": counts,`

```
{"index": "cindy_wu_catalog.streamco.knowledge_idx", "endpoint": "telco-vs-endpoint", "embedding_model": "system.ai.gte-large-en", "chunks": {"transcript": 20621, "policy": 13}, "citations": {"payment": "Save-offer policy 1. Payment failures: Involuntary (payment) cancellations rec", "playback": "Save-offer policy 2. Service failures: Subscribers with a verified service fai", "onboarding": "Save-offer policy 3. New event sign-ups: Event sign-ups in their first 30 days"}}
```
