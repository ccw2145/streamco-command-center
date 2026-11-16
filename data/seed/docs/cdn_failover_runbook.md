<!-- Owner: Stream C. Short static policy doc. Read by rules (notebooks/10, cc_common.policies) and shown as a cited snippet. Keep section headings stable: they are citation keys. -->
# CDN failover runbook (synthetic)

## 1. Scope

Applies to live-event streaming incidents where viewing quality degrades on one delivery network (CDN) in one metro.

## 2. Same-metro failover

When one device and app-version cohort degrades on a single CDN in a metro, move that cohort to the same-metro CDN with the most forecast headroom, provided the headroom exceeds the load being moved. Do not move a whole metro for a cohort-level fault.

## 3. Approval

A traffic move needs one approval from the on-call operations lead. Record the approver, the time and the cohort moved.

## 4. Verification

Confirm that rebuffering and start failures for the moved cohort return to within 1.5 times their baseline within 10 minutes. If they do not, roll back and escalate to the CDN vendor.
