# StreamCo Command Center: presentation deck (text export)

The slides as text, in order. The designed deck is `index.html` and `streamco-command-center.pdf`.

## Slide 1: Cover

- StreamCo Command Center
- Keep about 2,100 more subscribers after every big event
- $302k revenue protected for $42k, one cancel number every team agrees on, and capacity hot spots flagged before kickoff. Prepared for StreamCo leadership.
- Databricks | ConfidentialNovember 2026

## Slide 2: Problem

- The problem, to validate together
- Three gaps between your data and fast decisions
- Scattered data, debated numbers
- MySQL, Postgres and SaaS platforms that aren't connected yet. Product, Finance and Care each count cancels their own way, so decisions take longer than they should.
- Planning without forecasts
- Capacity, staffing and retention are planned from past peaks and spreadsheets. A forecast, a churn score and a way to learn from outcomes would help.
- Knowledge that's hard to use
- Years of policies, support transcripts and customer history could do more. Outreach is one offer for everyone, or manual work that's hard to scale.

## Slide 3: Architecture

- Proposed architecture
- Four layers on one governed platform, each built on the one before
- 1
- Governed data foundation
- MySQL, Postgres, Salesforce, payment and viewing data unified around the subscriber in Unity Catalog. Lakeflow checks quality; one Metric View defines every metric.
- 2
- Predictive ML
- A viewership forecast for capacity and a churn score for every subscriber, registered in Unity Catalog, monitored for drift and retrained on outcomes.
- 3
- GenAI
- An agent explains why, from certified metrics, your policies and support transcripts. Every at-risk subscriber gets a personal message that follows your policy.
- 4
- Command Center
- One screen per decision for leaders. Scores and messages are served from Lakebase in milliseconds, and approvals are written back.
- Governed throughout
- Unity Catalog lineage, quality rules and governed views that mask personal data and exclude kids. Unity Gateway logs, budgets and guards every AI call.

## Slide 4: What we built

- What we built
- One big night, from raw data to an approved plan
- Layer | What we built | What it showed
- Data foundation | Five source systems unified around the subscriber, with quality rules and one Metric View | Three team counts became one governed number, within 0.25% of the ledger
- Predictive ML | Viewership forecast and churn model, monitored and retrained in Unity Catalog | Dallas flagged at 97% before kickoff; about 30,000 subscribers at risk found
- GenAI | An agent over metrics, policies and transcripts; a personal message per subscriber | The why in about 90 seconds; 28,269 messages for about $58
- Command Center | Four screens; subscriber lookups and approvals in Lakebase; a person approves | About 2,100 saved: $302k protected for $42k, about 7x
- For the executive sponsor
- About $20 per subscriber saved, 7x return on the plan, and the meeting starts from one agreed number.
- For the domain owners
- Retention: save rates of 50%, 25% and 35% by cause. Operations: Dallas flagged before kickoff. Data: 99.7% of rows pass quality rules.
- Synthetic demo data. Assumes $11.99 monthly ARPU, 12 months, gross revenue.

## Slide 5: Next steps

- Next steps
- Prove it on your data in a 30-day proof of concept
- 1
- Week 1: agree
- A sponsor, success criteria and one cancel definition to certify, in a mutual plan.
- 2
- Week 2: connect
- Subscriptions, billing, support and viewing data in a sandbox under your governance.
- 3
- Week 3: build
- Forecast, churn model and the agent on your metrics and documents, hands-on.
- 4
- Week 4: decide
- Results against the criteria, and a go or no-go for the February game.
- The proof of concept
- Led by your Databricks account team and AI specialists, at no services cost.
- Then, to production
- Databricks Professional Services or a certified partner. Partner work can qualify for co-funding.
- Over two years
- Care staffing, always-on churn, personalization and ad insights reuse the same platform.
