# StreamCo Command Center: Demo Script

**Presenter:** Cindy Wu, Databricks, with the AE
**In the room:** the business leader, the CDO (data leadership), the VP or Director of Engineering
**Session:** 60 min. The content is about 33 min (slides, then the demo, then back to the slides). The rest is introductions, questions, pushback and the wrap-up.

**Session plan:**

| Part | Where | Time |
|---|---|---|
| Introductions and agenda (AE leads; no company intro or platform overview) | — | 0:00–3:00 |
| Opening: why we're here | Slide 1 | 3:00–4:00 |
| Problem: discovery and validation | Slide 2 | 4:00–11:00 |
| Proposed architecture: four layers | Slide 3 | 11:00–13:00 |
| The scenario: one big night | App, One Truth | 13:00–14:00 |
| Demo | App | 14:00–32:00 |
| What we built | Slide 4 | 32:00–33:30 |
| Next steps and the ask | Slide 5 | 33:30–36:00 |
| Questions and pushback | Slide 3 or 5 | 36:00–55:00 |
| Wrap-up | Slide 5 | 55:00–60:00 |

**Storyline:** three problems, four layers, one night. Each layer is built on the one before it.

| Problem | Layer | Screen | The one moment to land |
|---|---|---|---|
| Scattered data, debated numbers | 1. Governed data foundation | **One Truth** | Three team numbers become one: **15,800** |
| Planning without forecasts | 2. Predictive ML | **Forecast**, then **Save Subscribers** | Dallas flagged **before kickoff**; **30,000** at risk found before they cancel |
| Knowledge that's hard to use | 3. GenAI | **Ask**, then **Save Subscribers** | **The why in 90 seconds**, citing your own policy; a personal message for each subscriber |
| All three, for a leader | 4. Command Center | the app itself | **A plan a person approves** |

**The thread:** follow **Marcus** through all four layers. His account, billing and viewing records land in the foundation (1). The churn model scores him at risk (2). The agent writes him a message that follows your policy (3). A person approves the plan on the Command Center (4). Say his name once per layer.

**Each screen answers the next question of the morning:** 1 What happened? → 2 Could we have seen it coming? → 3 Why did they cancel? → 4 Who's next, and what do we do? The question is in each screen's header.

**Rule:** one point per screen. Don't tour the screen; point at one thing, say one line, move on.

> **SAY** = lines to deliver (bold = land word for word) · **DO** = click · ⏱ = clock · 👤 = persona line, use it when that person reacts or asks.
> All numbers are synthetic. Say so once, up front.

---

## Before you start

- App open on **One Truth**, press `R` to reset; drawers closed.
- Deck open on slide 1 (presenting). The app is in a second window, ready to switch to.
- Dry-run the Ask question once so it's cached.
- Know who is who: the business leader, the CDO, the VP of Engineering. Note what each said to the AE beforehand.

---

## 1. Opening: why we're here (⏱ 3:00–4:00) · Slide 1

No company intro and no platform overview. Go straight to their problem, in their words.

**SAY:**
> "*[AE name]* shared what you told us: the business is growing, and you want decision-making to keep pace with it. Today your data lives across Postgres, MySQL and several SaaS tools that aren't connected yet, so meetings often start by reconciling numbers, and decisions take longer than they should. Planning for capacity, staffing and retention leans on experience and spreadsheets, because there isn't a forecasting layer yet. And you have years of documents and customer history that could be doing a lot more for you.
>
> Before we show you anything, we'd like to check we've understood that right, and hear which part matters most to you."

---

## 2. Problem: discovery and validation (⏱ 4:00–11:00) · Slide 2

**DO:** show slide 2: the three problems.

Ask one question per card, left to right (~2 min each). Aim each question at the person who owns it. Capture one number and one quote per gap for the close.

1. **Scattered data, debated numbers** (to the CDO): "When Product, Finance and Care report the same metric, say yesterday's cancels, do they bring the same number? How long does it take to settle which one is right?"
2. **Planning without forecasts** (to the business leader): "How do you plan capacity, Care staffing and retention offers today: last year's peak and a spreadsheet, or a forecast? When an offer works, how do you know?"
3. **Knowledge that's hard to use** (to the VP of Engineering): "Where do your policies, support transcripts and customer history live today? Have you had a chance to put AI on them yet, and what would you want it to do?"

**Validate (1 min). SAY:**
> "So what I heard is…" *(read back 2–3 quotes)* "…Did I get that right? Which of the three matters most?"

> If they say "we already do that well": "Great, tell me if what you'll see is better or worse," and shorten that scene in the demo.

---

## 3. Proposed architecture: four layers (⏱ 11:00–13:00) · Slide 3

**DO:** show slide 3.

**SAY (keep it high level; one line per layer):**
> "Here's how we'd close those gaps: four layers on one governed platform, each built on the one before.
> 1. **Governed data foundation:** your MySQL, Postgres, Salesforce, payment and viewing data land in Unity Catalog, unified around one core entity, the subscriber. Pipelines check quality on the way in, and one Metric View defines every metric. That closes the first gap.
> 2. **Predictive ML, built on that foundation:** a viewership forecast for capacity, and a churn score for every subscriber. Both are monitored and retrained as outcomes come in. That closes the second.
> 3. **GenAI, built on both:** an agent that explains why, from your certified metrics, your policies and your support transcripts, and a personal message for every subscriber at risk. That closes the third.
> 4. **The Command Center:** what a leader actually looks at. One screen per decision.
>
> Governance isn't a separate layer. It runs underneath all four: lineage, quality rules and masking on the data, and every AI call logged and budgeted.
>
> **The layers compound: the model is only as good as the data, and the AI is only as useful as what's underneath it.**"

> 👤 **VP of Engineering:** "No rebuild: your sources connect through Lakeflow Connect or federation, and every model call goes through Unity Gateway." Save the details for Q&A.
> 👤 **CDO:** "Governance is in the data itself, not in each app, so every layer inherits it."

---

## The scenario: one big night (⏱ 13:00–14:00) · switch to the app

**DO:** switch to the app, on **One Truth**.

**SAY:**
> "To make it concrete, we built this around a night every streaming business knows: the biggest live game of the year, the Big Game, on Valentine's Day. At halftime, nine point six million people are streaming at once. That's a great night for the business.
>
> So what happens the next morning? It's usually when the three challenges we just talked about show up together. Three teams arrive with three slightly different cancel numbers. It's hard to tell whether the capacity issue could have been seen coming, or which subscribers might leave next. And the follow-up to subscribers tends to be the same email for everyone.
>
> Let's walk through that morning, layer by layer. Everything you'll see is synthetic data shaped like your business."

---

## 4. Demo (⏱ 14:00–32:00) · App

### One Truth: Layer 1, the governed data foundation (⏱ 14:00–18:00)

**DO:** point left to right across the source map.

**SAY:**
> "A subscriber's story lives in five systems: MySQL, Postgres, Salesforce, the payment processor and streaming telemetry. Here they land in one governed place, Unity Catalog, joined on one subscriber ID: 3.9 million subscribers, and every ID matched across all five systems. Marcus, who you'll meet later, is one of those subscribers."

**DO:** point at **Three teams, three numbers**.

**SAY (slow down):**
> "Yesterday's cancels, the way each team counts them: **Product 17,108, Finance 15,760, Care 12,851.** Nobody's wrong; they count different things, and now you can see exactly how.
> - **Product counts clicks.** About 4,500 people clicked cancel but stayed or came back within a day, and about 3,200 left without clicking at all, almost all because a payment failed.
> - **Care only sees cancels that came through a support contact**, so it misses about 2,900 who cancelled on their own.
> - **Finance's ledger is just 40 behind**, waiting on charges to post.
>
> Put those together and every team lands on the same **15,800**: subscriptions that actually ended, net of anyone who came back within 24 hours.
>
> **That's the meeting where the number is settled before it starts.** And hold on to those 3,200 failed payments; they come back in a minute."

**DO:** click **15,800** to show its SQL, lineage and quality checks, then close it.

**SAY:**
> "And you can trust it: here's where it came from, source to bronze, silver, gold and the Metric View. Six quality rules check the data on the way in, and 99.7% of rows passed. The one rule that flags rows, telemetry without an app version, is shown, not hidden."

> 👤 **CDO:** "Lineage and quality come from Unity Catalog and the pipelines, not from a document someone maintains. If a rule fails, the number shows it."
> 👤 **Business leader:** "This is what turns a week of reconciling into a number you can use in the meeting."

---

### Forecast: Layer 2, predictive ML (⏱ 18:00–20:30)

**DO:** click **Forecast**, point at the callout.

**SAY:**
> "Built on that same data: before kickoff, the forecast predicted the halftime peak by city and delivery network, and **flagged Dallas at 97% of capacity.** At halftime, that exact spot went red. Ops fixed it in 19 minutes.
>
> The natural next question is a harder one: **did that night affect subscribers?**"

> 👤 **Business leader:** "The same forecast pattern plans Care staffing and content capacity. Capacity is just the first use."

---

### ❓ Pause for questions (⏱ 20:30–22:00)

**SAY:** "Before we get to churn, which of these two is more painful for you today?"

---

### Ask: Layer 3, GenAI on your knowledge (⏱ 22:00–26:00)

**DO:** click **Ask**, click the suggested question, **Ask**.

**SAY (while it runs):**
> "The next morning I just type: **'Why did Sports cancellations jump?'** It pulls the numbers through a Genie Agent your analysts can use directly, breaks the cancels down by cause across viewing, payment and support data, and searches your policies and support transcripts. Usually that takes a few teams and a few days to piece together."

**SAY (when the answer lands):**
> "**Nearly two-thirds of the extra cancels trace to the same 20 minutes of halftime.** 34% couldn't watch. **29% signed up at halftime but their payment timed out and was never retried**, the failed payments Product's count couldn't see, and here it quotes your own payment retry policy, which says it should have been. 30% are normal event-only cancels. 7% it can't explain, and it says so."

**DO:** click **29%** to show the SQL and the policy passage.

**SAY:** "**Every number has its source one click away, and every claim about policy quotes the document.** It can only use what its tools returned."

> 👤 **VP of Engineering:** "Three tools: Genie for numbers, a governed function for the breakdown, and AI Search over the documents. Every step is traced in MLflow."
> 👤 **CDO:** "Transcripts are searched after masking, with kids excluded, so the AI never sees what a person couldn't."

---

### Save Subscribers: Layers 2, 3 and 4 together (⏱ 26:00–31:00)

**DO:** click **Save Subscribers**.

**SAY:**
> "That's who already left. The more valuable question: **who's about to? About 30,000 subscribers are likely to cancel and haven't yet.** The churn model found them using the signals we just saw."

**DO:** point at the model strip.

**SAY:**
> "And it doesn't go stale. Accuracy is holding at 0.80, but last night changed viewing patterns, so drift monitoring has already flagged a retrain. A new version only replaces this one if it beats it on the same test. And the save rates come from real holdout tests, **so the plan learns from outcomes.**"

**DO:** point at the totals row (skip the individual cards).

**SAY:**
> "The plan matches the action to the cause: fix the failed payments, make good on the buffering, welcome the new sign-ups. **About 2,100 saved, $42,000 cost, $302,000 protected.**"

**DO:** scroll to **Marcus and Priya**.

**SAY (the GenAI moment):**
> "And each of them gets a message written from what happened to *them*, following the policy it retrieved. Marcus lost the halftime show to buffering; his message owns it and suggests the next sports doc. Priya's card was declined by our processor; hers says it's fixed, no discount needed, and her show's new season is waiting.
>
> **Same plan, different conversations, all within your offer policy, with kids excluded and personal data masked.**"

**DO:** click **Approve plan**.

**SAY:** "**Nothing is sent until a person approves.** That's the Command Center: from five systems last night to a plan a leader signs off this morning."

> 👤 **Business leader:** "One offer for everyone becomes a conversation that fits each subscriber, at a few cents each."
> 👤 **VP of Engineering:** "One batch job writes every message through the gateway; Regenerate shows it's live."

---

### Governance (⏱ 31:00–32:00): *one line*

**DO:** click **Governed by Unity Gateway** in the header.

**SAY:**
> "And every AI call you saw today is logged, budgeted per team and governed by the same Unity Catalog as the data."

**DO:** close the drawer.

---

## 5. Back to the slides: what we built (⏱ 32:00–33:30) · Slide 4

**DO:** switch back to the deck, slide 4.

**SAY:**
> "One night, one row per layer: **one governed number instead of three, Dallas flagged before kickoff and 30,000 at risk found, the why in 90 seconds with personal messages for each, and about 7 to 1 on the plan a person approved.** And it repeats every live event."

---

## 6. Next steps and the ask (⏱ 33:30–36:00) · Slide 5

**SAY:**
> "Earlier you said *[their quote]*. Here's how we'd prove it on your data, in 30 days:
> - **Week 1:** a sponsor, success criteria and one cancel definition to certify.
> - **Week 2:** connect subscriptions, billing, support and viewing data in a sandbox, under your governance.
> - **Week 3:** the forecast, the churn model and the agent on your metrics and documents, with your teams hands-on.
> - **Week 4:** results against the criteria, and a go or no-go for the February game.
>
> **Your Databricks account team and our AI specialists run the proof of concept, at no services cost.** For production, Databricks Professional Services or a certified partner builds it, and partner work can qualify for Databricks co-funding. Over two years, Care staffing, always-on churn, personalization and ad insights all reuse the same platform."

**THE ASK:**
> "**What I need today is a sponsor for the 30-day proof of concept, and one person each from engineering, subscriber data and Care.** Who should I work with?"

> **If asked "what does co-funding mean?":** "Databricks can co-fund a share of a partner's delivery for a named production use case. Your account team scopes it after the proof of concept." Don't promise an amount.

---

## 7. Questions and pushback (⏱ 36:00–55:00)

**Stance:** don't get defensive. For every pushback: **agree with the concern → show where the demo already handles it → offer to prove it in the 30-day POC as a success criterion.** Go back to slide 3 for technical questions.

### Cost

| Who | Pushback | Answer |
|---|---|---|
| Business leader | "What will this cost us?" | "Fair question. Two parts: the POC is at no services cost, and run cost is usage-based. The 28,269 personal messages tonight cost about $55, and every AI call has a budget per team. Let's put a cost-per-save target in the success criteria." |
| CDO | "We already pay for a warehouse, BI and a CRM." | "Keep the ones that work. The foundation reads from what you have; the saving is fewer copies of the data and fewer tools to reconcile, which we can size in Week 1." |
| VP of Engineering | "AI costs spiral." | "Agreed, that's why every call here goes through Unity Gateway with a budget and a log. You saw the spend in the drawer. Nothing runs outside it." |

### Complexity

| Who | Pushback | Answer |
|---|---|---|
| VP of Engineering | "My team can't take on another platform." | "Understood. This replaces stitching, it doesn't add to it: one place for ingestion, models, AI and access rules. In the POC your engineers build it with us, so you can judge the effort first-hand." |
| VP of Engineering | "Do we have to migrate MySQL and Postgres?" | "No. They stay where they are and connect through Lakeflow Connect or federation." |
| Business leader | "This looks like a big program." | "It starts as one night and one decision, 30 days, with a go or no-go. Each next use case reuses the same data and controls." |

### Trust in the data

| Who | Pushback | Answer |
|---|---|---|
| CDO | "How do I know 15,800 is right?" | "You decide which definition to certify; the point is everyone uses that one. It reconciles to the Finance ledger within 0.25%, and you can trace every row through lineage and the quality rules." |
| CDO | "Who sees personal data?" | "Every screen, model and prompt reads through governed views in Unity Catalog that mask personal data and drop kids profiles. Only a privileged group sees unmasked values. In production we'd move the same rules onto the tables as column masks and row filters, so nothing can go around them." |
| Business leader | "Is this scripted?" | "This run is recorded so the demo is reliable. In the POC it runs live on your data, with the same rules." |
| VP of Engineering | "Are those real connectors?" | "Simulated here. In the POC they're real connections to your MySQL, Postgres and Salesforce." |

### AI reliability

| Who | Pushback | Answer |
|---|---|---|
| Business leader | "What if the AI is wrong?" | "It can only use numbers its tools return, it quotes the document for any policy claim, it declines when sources disagree, and people approve every action." |
| CDO | "Correlation isn't causation." | "Agreed. Each driver shows its evidence: the payment share is seen directly in decline codes, and the 7% is left unexplained rather than forced." |
| VP of Engineering | "Models drift." | "They do, and you saw it: last night's viewing shifted enough that monitoring flagged a retrain. The weekly retrain job trains a challenger, and it's promoted only if it beats the champion on the same test." |
| VP of Engineering | "Which model? Lock-in?" | "Every call goes through Unity Gateway, so the model is a setting you can swap." |
| Any | "We already have monitoring, anomaly detection and Genie." | "Good, this works with them. The new parts are the forecast before the game and connecting incidents to churn after it." |

---

## 8. Wrap-up (⏱ 55:00–60:00) · Slide 5

**SAY:**
> "To recap what I heard: *[their top gap and quote]*. What you saw: one governed number, a forecast and a churn score built on it, and AI that uses your own documents to act on both, with a person approving. Next: the 30-day proof of concept. Can we confirm the sponsor and the three people today, and book Week 1?"

---

## If you're running long

Cut in this order: the 15,800 click → the 29% click → the governance drawer (say the line without opening it) → the mid-demo pause (take questions at the end).

**Never cut:** the three team numbers, "flagged before kickoff", the Ask answer with the policy quote, the 30,000 pivot, the personalized messages, Approve.

## Optional (only if asked or ahead of time)

- **Originals follow-up on Ask:** it declines to answer because two sources disagree by 6%.
- **Regenerate** on a personalized message (shows it's live).
- **How it works** (`H`) on any screen, for the VP of Engineering.

## If something breaks

- Screen hangs: refresh. The app falls back to the recorded run automatically (small "recorded" dot).
- App won't load: stay on the slides. Talk through slide 3 as the demo, then go to 4–5, and offer a recording.

## After the meeting

Send the deck PDF, recap their quotes and the next step, and book the first validation session.
