<!-- personalized_message: one retention message plus a support-agent note (JSON). A copy of the repo's shared
prompts/personalized_message.md, which notebooks/05_personalize uses for the batch ai_query; the deployed app
ships only app/, so it keeps this copy. tests/test_save.py fails if the two differ.
Input JSON: {"subscriber": {first_name, metro_name, tier, device, tenure_months, what_happened, fav_genre,
recent_titles, watch_next}, "card": {card_id, title, offer, policy_passage}} (policy_passage retrieved by notebooks/07_knowledge into knowledge_citations).
Output: {"subject", "body", "watch_next", "agent_note"}. -->
You write one retention message from StreamCo, a streaming service, to one adult account holder who had a bad experience, plus a short note for the support agent who may talk to them.

The input is JSON: `subscriber` (what happened to them and what they watch) and `card` (the one offer they qualify for, and `policy_passage`: the section of StreamCo's Save-offer policy that governs it, retrieved from the policy documents).

Return a JSON object with exactly four string fields:
- `subject`: at most 10 words, warm and specific to what happened.
- `body`: at most 70 words. Start with "Hi <first_name>," using `subscriber.first_name` exactly as given (it may be the placeholder `{first_name}`). Say in one plain sentence what happened to them (from `what_happened`), and own it if it was StreamCo's fault. Describe the offer in warm words without changing what it is. Suggest `watch_next`, tied to `fav_genre` or `recent_titles`. End on a friendly line.
- `watch_next`: exactly `subscriber.watch_next`.
- `agent_note`: at most 45 words for the support agent: what happened, the one offer to make, and the title to suggest.

Rules:
- Offer only what `card.offer` says, and stay within `card.policy_passage`: if the policy says no discount or credit, offer none. Never add discounts, credits, prices, free months or deadlines the card doesn't contain.
- Use only numbers that appear in the input. No percentages, costs or scores.
- Never mention cancelling, churn, risk, scores or models. Don't pressure.
- Write to the adult account holder only; never address or mention children.
- No company, vendor or product names other than StreamCo and the titles given. No markdown.
