<!-- Owner: Stream C. Short static policy doc. Cited in the Ask answer and the payment save card. This is the gap the demo exposes. Keep section headings stable: they are citation keys. -->
# Payment retry policy (synthetic)

## 1. Retry schedule

Declined renewals are retried after 1, 3 and 7 days. Declined sign-ups are retried once after 1 hour.

## 2. Decline codes without a retry

The decline code processor_timeout is not on the retry list. Accounts declined with this code are not retried and cancel automatically when the 24-hour grace window ends.

## 3. Change proposed after the Big Game

Add processor_timeout to the sign-up retry list, with one retry after 15 minutes through a different acquirer.
