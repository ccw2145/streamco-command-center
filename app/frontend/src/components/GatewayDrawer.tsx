// "Governed by Unity Gateway": AI calls, cost and budgets; masked vs original transcript; the Ask trace.
import { Fragment, useMemo } from "react";
import { motion } from "framer-motion";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { compact, count, pct, usdExact, usdShort } from "@/lib/format";
import type { GovernancePayload } from "@/lib/types";
import { DrawerFrame, Panels, Section } from "./Drawer";
import { ErrorState, Skeleton } from "./ui";

export function GatewayDrawer({ open, onClose }: { open: boolean; onClose: () => void }) {
  const q = useQuery({ queryKey: ["screen", "governance"], queryFn: () => api.screen("governance"), enabled: open, staleTime: 60_000 });
  return (
    <DrawerFrame open={open} onClose={onClose} eyebrow="Governance across every screen" title="Governed by Unity Gateway">
      {q.isPending ? (
        <div className="space-y-4" role="status" aria-label="Loading">
          <Skeleton className="h-24" />
          <Skeleton className="h-40" />
        </div>
      ) : q.isError ? (
        <ErrorState compact error={q.error} onRetry={() => q.refetch()} />
      ) : (
        <Body d={q.data} />
      )}
    </DrawerFrame>
  );
}

function Body({ d }: { d: GovernancePayload }) {
  const over = d.budgets.filter((b) => b.status === "alert");
  const maxCalls = Math.max(1, ...d.usage.by_service.map((s) => s.calls));
  const totalCost = d.usage.costs.reduce((s, c) => s + c.usd, 0);
  return (
    <>
      <p className="text-[0.98rem] leading-relaxed text-ink/90">
        Every AI call in this demo goes through one gateway: the Ask agent, the Genie Agent and the personalized messages.
        Personal data is masked and kids profiles are filtered before any model sees them.
      </p>

      <Section title="Usage and cost" note={d.seeded_note}>
        <div className="grid grid-cols-3 gap-3">
          <Big value={count(d.usage.calls)} label="calls through the gateway" />
          <Big value={count(d.usage.redactions)} label="personal details masked" />
          <Big value={usdExact(totalCost)} label="AI cost for this demo" />
        </div>
        <ul className="mt-4 space-y-2">
          {d.usage.by_service.map((s) => (
            <li key={s.service} className="grid grid-cols-[11rem_1fr_3rem] items-center gap-3 text-sm">
              <span className="truncate text-muted">{s.service}</span>
              <span className="h-2 rounded-full bg-raised">
                <span className="block h-full rounded-full bg-ink/70" style={{ width: `${(s.calls / maxCalls) * 100}%` }} />
              </span>
              <span className="tnum text-right">{count(s.calls)}</span>
            </li>
          ))}
        </ul>
        <ul className="mt-4 divide-y divide-line rounded-xl border border-line">
          {d.usage.costs.map((c) => (
            <li key={c.label} className="flex items-baseline justify-between px-4 py-2.5 text-sm">
              <span className="text-muted">{c.label}</span>
              <span className="tnum font-medium">{usdExact(c.usd)}</span>
            </li>
          ))}
        </ul>
        {d.live_usage && (
          <div className="mt-3 flex items-center justify-between rounded-xl border border-ok/30 bg-ok/[0.04] px-4 py-3 text-sm">
            <span className="inline-flex items-center gap-2 text-muted">
              <span className="h-1.5 w-1.5 rounded-full bg-ok" /> Gateway usage table
            </span>
            <span className="tnum">
              <span className="text-ink">{count(d.live_usage.calls)}</span> calls &middot;{" "}
              <span className="text-ink">{compact(d.live_usage.tokens)}</span> tokens &middot; {d.live_usage.label}
            </span>
          </div>
        )}
      </Section>

      <Section title="AI budget by team, this month">
        {over.length > 0 && (
          <div className="mb-4 flex items-center gap-3 rounded-xl border border-alert/30 bg-alert/[0.06] px-4 py-2.5 text-sm">
            <span className="h-2 w-2 shrink-0 rounded-full bg-alert" />
            <span>
              {over.map((b) => b.team).join(", ")} passed {pct(over[0].alert_pct)} of budget. The team owner was alerted; calls
              keep working until 100%.
            </span>
          </div>
        )}
        <div className="space-y-5">
          {d.budgets.map((b, i) => (
            <div key={b.team}>
              <div className="flex items-baseline justify-between">
                <span className="text-[1.02rem] font-medium">{b.team}</span>
                <span className="tnum flex items-baseline gap-3">
                  <span className="text-sm text-muted">{usdShort(b.spend_usd)} of {usdShort(b.monthly_budget_usd)}</span>
                  <span className={`text-num-sm ${b.status === "alert" ? "text-alert" : "text-ink"}`}>{pct(b.budget_pct)}</span>
                </span>
              </div>
              <div className="relative mt-2.5 h-3 rounded-full bg-raised">
                <motion.div className={`h-full rounded-full ${b.status === "alert" ? "bg-alert" : "bg-ink/80"}`}
                  initial={{ width: 0 }} animate={{ width: `${Math.min(1, b.budget_pct) * 100}%` }}
                  transition={{ delay: 0.1 + i * 0.1, duration: 0.8, ease: "easeOut" }} />
                <div className="absolute -bottom-1 -top-1 w-[2px] rounded bg-faint" style={{ left: `${b.alert_pct * 100}%` }} />
              </div>
            </div>
          ))}
        </div>
      </Section>

      <Masking m={d.masking} />

      <Section title="Trace of the Ask run" note="MLflow traces stored in Unity Catalog.">
        <div className="flex items-start justify-between gap-4 rounded-xl border border-line p-4">
          <div>
            <div className="font-medium">{d.trace.label}</div>
            <div className="tnum mt-1 text-sm text-muted">{d.trace.steps} steps &middot; {d.trace.duration_s}s</div>
          </div>
          {d.trace.url ? (
            <a className="btn-ghost shrink-0" href={d.trace.url} target="_blank" rel="noreferrer">Open in MLflow &rarr;</a>
          ) : (
            <span className="shrink-0 text-xs text-faint">Open from the MLflow experiment</span>
          )}
        </div>
      </Section>

      <div className="border-t border-line pt-7">
        <Panels hiw={d.how_it_works} />
      </div>
    </>
  );
}

function Big({ value, label }: { value: string; label: string }) {
  return (
    <div className="rounded-xl bg-raised p-4">
      <div className="tnum text-[1.6rem] font-semibold leading-none tracking-tight">{value}</div>
      <div className="mt-2 text-xs leading-snug text-muted">{label}</div>
    </div>
  );
}

const TOKEN = /(\[[A-Z_]+\])/g;
const IS_TOKEN = /^\[[A-Z_]+\]$/;

/** Highlight the original's personal details by aligning it with the masked text's literal segments. */
function alignPii(masked: string, original: string): { text: string; pii: boolean }[] | null {
  const parts = masked.split(TOKEN).filter((p) => p !== "");
  const out: { text: string; pii: boolean }[] = [];
  let pos = 0;
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    if (IS_TOKEN.test(p)) {
      const next = parts.slice(i + 1).find((x) => !IS_TOKEN.test(x));
      const end = next !== undefined ? original.indexOf(next, pos) : original.length;
      if (end < pos) return null;
      out.push({ text: original.slice(pos, end), pii: true });
      pos = end;
    } else {
      if (original.slice(pos, pos + p.length) !== p) return null;
      out.push({ text: p, pii: false });
      pos += p.length;
    }
  }
  if (pos < original.length) out.push({ text: original.slice(pos), pii: false });
  return out;
}

function Masking({ m }: { m: GovernancePayload["masking"] }) {
  const aligned = useMemo(() => alignPii(m.masked_text, m.unmasked_text), [m]);
  const restricted = m.masked_text === m.unmasked_text;
  return (
    <section aria-label="Masked transcript">
      <div className="flex items-baseline justify-between gap-4">
        <h3 className="font-semibold">A support transcript, before and after masking</h3>
        <span className="tnum shrink-0 text-sm text-muted">{m.n_redactions} items masked</span>
      </div>
      <div className="mt-4 grid gap-3">
        <div className="rounded-xl border border-ok/30 bg-ok/[0.04] p-4">
          <div className="eyebrow !text-ok">What the AI sees</div>
          <p className="mt-3 text-[0.92rem] leading-relaxed text-ink/90">
            {m.masked_text.split(TOKEN).map((p, i) =>
              IS_TOKEN.test(p) ? (
                <span key={i} className="mx-0.5 rounded bg-ok/15 px-1.5 py-0.5 font-mono text-[0.78em] text-ok">{p.slice(1, -1)}</span>
              ) : (
                <Fragment key={i}>{p}</Fragment>
              ),
            )}
          </p>
        </div>
        <div className="rounded-xl border border-line p-4">
          <div className="eyebrow">Original &middot; restricted access</div>
          <p className="mt-3 text-[0.92rem] leading-relaxed text-muted">
            {restricted
              ? "Visible only to members of cc_privileged."
              : aligned
                ? aligned.map((s, i) =>
                    s.pii ? (
                      <span key={i} className="rounded bg-raised px-1 text-ink underline decoration-alert/60 decoration-2 underline-offset-4">{s.text}</span>
                    ) : (
                      <Fragment key={i}>{s.text}</Fragment>
                    ),
                  )
                : m.unmasked_text}
          </p>
        </div>
      </div>
      <p className="mt-3 text-xs text-faint">Masking happens in governed views that every screen, model and prompt reads through. All personal data here is synthetic.</p>
    </section>
  );
}
