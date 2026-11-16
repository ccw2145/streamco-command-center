// Save Subscribers: 31k headline + model strip; three light plan cards + a strong totals row; Marcus and Priya's
// messages (Regenerate = one live LLM call), each with its live Lakebase row; the batch card; Approve plan (writes
// the approvals table in Lakebase).
import { useEffect, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useScreen } from "@/lib/useScreen";
import { HowItWorksDrawer, KeyValue, Section } from "@/components/Drawer";
import { ProvenanceNumber } from "@/components/Provenance";
import { ApproveButton, Check, ErrorState, RecordedDot, ScreenSkeleton } from "@/components/ui";
import { api, friendlyMessage, RESET_EVENT } from "@/lib/api";
import { approx, compact, count, pct, usdExact, usdShort } from "@/lib/format";
import type { Approval, Message, ModelMonitoring, PlanCard, Sample, SavePayload } from "@/lib/types";

/** The script shows two of the three sample subscribers. */
const SHOWN = ["marcus", "priya"];

export function SaveSubscribers() {
  const q = useScreen("save");
  if (q.isPending) return <ScreenSkeleton tiles={3} />;
  if (q.isError) return <ErrorState error={q.error} onRetry={() => q.refetch()} />;
  return <SaveView d={q.data} />;
}

function SaveView({ d }: { d: SavePayload }) {
  const qc = useQueryClient();
  const h = d.headline;
  const m = d.model_card;
  const mon = m.monitoring ?? null;
  const [approval, setApproval] = useState<Approval | null>(d.approval);
  useEffect(() => {
    const reset = () => setApproval(null);
    window.addEventListener(RESET_EVENT, reset);
    return () => window.removeEventListener(RESET_EVENT, reset);
  }, []);
  const approve = async () => {
    const a = await api.approve();
    setApproval(a);
    qc.setQueryData(["screen", "save"], { ...d, approval: a });
  };

  return (
    <div>
      <header className="flex items-end justify-between gap-10">
        <div className="max-w-[62rem]">
          <div className="eyebrow">4 of 4 &middot; Who&rsquo;s next, and what do we do?</div>
          <h1 className="mt-3 text-[2.3rem] font-semibold leading-[1.12] tracking-tight">
            <ProvenanceNumber metricId="subs_at_risk">{count(h.at_risk)}</ProvenanceNumber> subscribers are likely to cancel. Here&rsquo;s the plan.
          </h1>
          <div className="mt-3 flex flex-wrap items-center gap-x-5 gap-y-2 text-sm">
            <span className="inline-flex items-center gap-2 rounded-full border border-line bg-raised px-3 py-1">
              <span className="text-muted">Churn model</span>
              <span className="font-mono text-xs text-ink" title={m.uc_name}>{m.uc_name.split(".").pop()}@{m.alias}</span>
              <span className="text-faint">v{m.version}</span>
            </span>
            <span className="text-muted">AUC <ProvenanceNumber metricId="churn_model_auc" className="tnum text-ink">{m.auc.toFixed(2)}</ProvenanceNumber></span>
            {mon && <span className="text-muted">trained through <span className="text-ink/90">{monthYear(mon.recent_window)}</span></span>}
            {mon && <DriftPill mon={mon} />}
          </div>
          {mon && <p className="mt-1.5 text-xs text-faint">The model {mon.retrain_short}; a challenger must beat the champion before it&rsquo;s promoted.</p>}
          <p className="mt-2 text-sm text-muted">Top drivers: <span className="text-ink/90">{m.top_drivers.join(" · ")}</span></p>
          <p className="mt-3 text-[1.1rem] text-muted">
            Without action, about <span className="tnum text-ink">{count(h.projected_cancels_30d)}</span> more cancel in the next 30 days.
            Revenue at risk if no action: <ProvenanceNumber metricId="revenue_at_risk" className="tnum text-ink">{usdShort(h.revenue_at_risk_usd)}</ProvenanceNumber>.
          </p>
        </div>
        <div className="shrink-0">
          <ApproveButton approved={approval !== null} onApprove={approve} hint="Nothing is sent until you approve." />
          {approval && (
            <p className="mt-2 flex items-center justify-end gap-2 text-right text-xs text-ok">
              <RecordedDot show={approval.recorded} label="kept in this session" />
              {approval.stored === "lakebase" ? "Recorded in Lakebase" : ""} &middot; support notes queued
            </p>
          )}
        </div>
      </header>

      <div className="mt-7 grid grid-cols-3 gap-5">
        {d.cards.map((c, i) => <Card key={c.card_id} card={c} index={i} approved={approval !== null} />)}
      </div>

      <section className="card mt-5 grid grid-cols-3 items-center gap-6 px-7 py-5" aria-label="Plan totals">
        <Total label="Plan saves">
          <ProvenanceNumber metricId="plan_saved_total">{approx(h.saved_point)}</ProvenanceNumber>
          <span className="ml-2 text-sm font-normal text-muted">({count(Math.round(h.saved_lo / 100) * 100)}–{count(Math.round(h.saved_hi / 100) * 100)})</span>
        </Total>
        <Total label="Costs"><ProvenanceNumber metricId="plan_cost">{usdShort(h.cost_usd)}</ProvenanceNumber></Total>
        <Total label="Protects"><ProvenanceNumber metricId="plan_revenue_protected">{usdShort(h.revenue_protected_usd)}</ProvenanceNumber></Total>
      </section>

      <section className="mt-8" aria-label="Personalized messages">
        <div className="flex items-end justify-between gap-8">
          <div>
            <div className="eyebrow">Personalized for each subscriber</div>
            <h2 className="mt-2 text-[1.6rem] font-semibold tracking-tight">Different people, different messages</h2>
            <p className="mt-1 text-[0.98rem] text-muted">Written from what happened to them and what they watch. Every message stays within its plan&rsquo;s offer.</p>
          </div>
          <div className="card flex shrink-0 items-center gap-6 px-6 py-4" aria-label="Batch personalization run">
            <div>
              <div className="text-xs text-muted">Generated for all</div>
              <ProvenanceNumber metricId="personalization_run" className="tnum text-num-sm">{count(d.run.n_messages)}</ProvenanceNumber>
            </div>
            <div className="h-10 w-px bg-line" />
            <div className="text-sm">
              <div><span className="tnum font-semibold">{usdExact(d.run.cost_usd)}</span>{" "}
                <span className="text-muted">for the batch &middot; {compact(d.run.n_messages)} messages in one <span className="font-mono text-xs">ai_query</span> run</span></div>
              <div className="mt-1 flex items-center gap-1.5 text-xs text-ok"><span className="h-1.5 w-1.5 rounded-full bg-ok" />PII masked · kids excluded · via Unity Gateway</div>
              <div className="mt-0.5 font-mono text-[0.7rem] text-faint">{d.run.model}</div>
            </div>
          </div>
        </div>
        <div className="mt-5 grid grid-cols-2 gap-5">
          {d.samples.filter((s) => SHOWN.includes(s.sample_id)).map((s, i) => (
            <SampleColumn key={s.sample_id} sample={s} card={d.cards.find((c) => c.card_id === s.card_id)} index={i} approved={approval !== null} />
          ))}
        </div>
      </section>

      <HowItWorksDrawer hiw={d.how_it_works}>
        <Section title="The model" note={d.assumptions}>
          <KeyValue items={[
            ["Registered as", <span className="font-mono text-xs">{m.uc_name}@{m.alias} (v{m.version})</span>],
            ["AUC", m.auc.toFixed(2)],
            ["Scored", `${count(m.scored)} eligible subscribers (active, not kids-only, not cancelled)`],
            ["At risk", `${count(h.at_risk)}; about ${count(h.projected_cancels_30d)} expected to cancel in 30 days`],
          ]} />
        </Section>
        {mon && <MonitoringDetail mon={mon} />}
        {mon && mon.outcomes.length > 0 && <OutcomesDetail mon={mon} />}
      </HowItWorksDrawer>
    </div>
  );
}

function Card({ card, index, approved }: { card: PlanCard; index: number; approved: boolean }) {
  const [why, setWhy] = useState(false);
  return (
    <motion.article className={`flex flex-col rounded-card border px-5 py-4 ${approved ? "border-ok/30" : "border-line"}`}
      initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.06 * index, duration: 0.3 }}>
      <div className="flex items-baseline justify-between gap-3">
        <h3 className="text-[1.05rem] font-semibold leading-tight">{card.title}</h3>
        <span className="tnum shrink-0 text-xs text-muted">{count(card.n_subscribers)} people</span>
      </div>
      <p className="mt-0.5 text-sm text-muted">{card.who} &middot; <span className="text-ink/80">{card.offer}</span></p>
      <div className="mt-3 flex items-baseline gap-x-5 text-sm">
        <span><span className="tnum text-[1.35rem] font-semibold">~{count(card.saved_point)}</span> <span className="text-muted">saved</span>{" "}
          <span className="tnum text-xs text-faint">({count(card.saved_lo)}–{count(card.saved_hi)})</span></span>
        <span className="text-muted"><span className="tnum text-ink/90">{usdShort(card.cost_usd)}</span> cost</span>
        <span className="text-muted"><span className="tnum text-ink/90">{usdShort(card.revenue_protected_usd)}</span> protected</span>
      </div>
      <div className="tnum mt-1 text-xs text-faint">of {count(card.expected_cancels)} expected to cancel &middot; {pct(card.save_rate)} save rate from past tests</div>
      <div className="mt-auto pt-3">
        <button className="flex items-center gap-2 text-xs text-ok" onClick={() => setWhy((v) => !v)} aria-expanded={why}>
          <Check className="h-3 w-3" /> Within offer policy
          {card.policy_source && <span className="text-muted">&middot; {card.policy_source}</span>}
          <span className="text-faint">{why ? "hide" : "why"}</span>
        </button>
        {why && <p className="mt-2 border-l-2 border-line pl-3 text-xs italic leading-relaxed text-muted">&ldquo;{card.policy}&rdquo;</p>}
      </div>
    </motion.article>
  );
}

function monthYear(iso: string): string {
  const d = new Date(`${iso.slice(0, 10)}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString("en-US", { month: "short", year: "numeric", timeZone: "UTC" });
}

const DRIFT_STYLE: Record<ModelMonitoring["status"], [string, string]> = {
  ok: ["Stable", "border-ok/40 bg-ok/10 text-ok"],
  watch: ["Drift: watch", "border-line bg-raised text-ink/90"],
  retrain: ["Drift: retrain due", "border-alert/40 bg-alert/10 text-alert"],
};

/** The monitoring status as found by notebooks/08_monitor; opens the PSI table. */
function DriftPill({ mon }: { mon: ModelMonitoring }) {
  const [label, cls] = DRIFT_STYLE[mon.status] ?? DRIFT_STYLE.watch;
  return (
    <ProvenanceNumber metricId={mon.metric_id} className="!no-underline">
      <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs ${cls}`} title={mon.status_reason}>
        {label}
        {mon.status !== "ok" && <span className="tnum opacity-80">&middot; PSI {mon.max_top_psi.toFixed(2)} {mon.max_top_psi_feature.toLowerCase()}</span>}
      </span>
    </ProvenanceNumber>
  );
}

function MonitoringDetail({ mon }: { mon: ModelMonitoring }) {
  const shown = mon.features.filter((f) => f.is_top || f.drift !== "low");
  return (
    <Section title="Monitoring and retraining" note={mon.retrain_rule}>
      <KeyValue items={[
        ["Status", <span className={mon.status === "retrain" ? "text-alert" : mon.status === "ok" ? "text-ok" : ""}>{DRIFT_STYLE[mon.status]?.[0] ?? mon.status}: {mon.status_reason}</span>],
        ["AUC", `${mon.auc_train.toFixed(3)} at training; ${mon.auc_recent.toFixed(3)} on the latest labelled month (${monthYear(mon.recent_window)}, ${count(mon.recent_window_n)} held-out subscribers)`],
        ["AUC by month", mon.auc_by_window.map((w) => `${monthYear(w.window)} ${Number(w.auc).toFixed(3)}`).join(" · ")],
        ["Champion", `v${mon.version}, registered ${mon.trained_at.slice(0, 10)}`],
        ["Challenger", mon.challenger_version ? `v${mon.challenger_version}` : "None yet. The weekly retrain job trains one and promotes it only if it beats the champion on the same held-out month."],
        ["Score drift", `PSI ${mon.score_psi.toFixed(2)} on the model's own score`],
      ]} />
      <table className="mt-4 w-full text-sm">
        <thead className="text-left text-xs text-muted">
          <tr><th className="py-1.5 font-medium">Feature (importance rank)</th><th className="py-1.5 text-right font-medium">Training</th>
            <th className="py-1.5 text-right font-medium">Post-game</th><th className="py-1.5 text-right font-medium">PSI</th></tr>
        </thead>
        <tbody className="tnum">
          {shown.map((f) => (
            <tr key={f.feature} className="border-t border-line">
              <td className="py-1.5 pr-3">{f.label} <span className="text-xs text-faint">#{f.rank}{f.is_top ? " · top 5" : ""}</span></td>
              <td className="py-1.5 text-right text-muted">{f.baseline_mean === null ? "—" : f.baseline_mean.toFixed(2)}</td>
              <td className="py-1.5 text-right text-muted">{f.current_mean === null ? "—" : f.current_mean.toFixed(2)}</td>
              <td className={`py-1.5 text-right ${f.drift === "high" ? "text-alert" : f.drift === "moderate" ? "text-ink" : "text-muted"}`}>{f.psi.toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-2 text-xs text-faint">Means shown; PSI compares the training months with the post-game snapshot ({mon.scoring_snapshot}). Above 0.2 is high drift.</p>
    </Section>
  );
}

function OutcomesDetail({ mon }: { mon: ModelMonitoring }) {
  return (
    <Section title="Where the save rates come from" note="Each card's save rate is the mean lift (treated minus holdout save rate) of its past pre-cancel holdout tests. New tests are added to the same table, so the next plan learns from this one.">
      <table className="w-full text-sm">
        <thead className="text-left text-xs text-muted">
          <tr><th className="py-1.5 font-medium">Card</th><th className="py-1.5 font-medium">Holdout tests</th>
            <th className="py-1.5 text-right font-medium">Save rate</th></tr>
        </thead>
        <tbody className="tnum">
          {mon.outcomes.map((o) => {
            const dates = o.tests.map((t) => t.run_date).sort();
            return (
              <tr key={o.card_id} className="border-t border-line align-top">
                <td className="py-2 pr-3">{o.title}</td>
                <td className="py-2 pr-3 text-muted">
                  <ProvenanceNumber metricId="save_rate_sources" className="text-muted">{o.tests.length} tests</ProvenanceNumber>, {monthYear(dates[0])}&ndash;{monthYear(dates[dates.length - 1])}
                  <div className="text-xs text-faint">lift {o.tests.map((t) => pct(t.lift)).join(" · ")}</div>
                </td>
                <td className="py-2 text-right">{pct(o.save_rate)}{Math.abs(o.save_rate - o.plan_save_rate) > 1e-6 && <div className="text-xs text-alert">plan uses {pct(o.plan_save_rate)}</div>}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </Section>
  );
}

function Total({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <div className="text-xs text-muted">{label}</div>
      <div className="tnum mt-1 text-num-sm">{children}</div>
    </div>
  );
}

function sentence(s: string): string {
  return s ? s[0].toUpperCase() + s.slice(1) + (/[.!?]$/.test(s) ? "" : ".") : s;
}

function SampleColumn({ sample, card, index, approved }: { sample: Sample; card?: PlanCard; index: number; approved: boolean }) {
  const [shown, setShown] = useState<{ msg: Message; live: boolean | null; note: string | null }>({ msg: sample.message, live: null, note: null });
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const reset = () => { setShown({ msg: sample.message, live: null, note: null }); setError(null); setVersion(0); };
    window.addEventListener(RESET_EVENT, reset);
    return () => window.removeEventListener(RESET_EVENT, reset);
  }, [sample.message]);

  const regenerate = async () => {
    if (pending) return;
    setPending(true);
    setError(null);
    try {
      const r = await api.regenerate(sample.sample_id);
      setShown({ msg: r.message, live: !r.recorded, note: r.note });
      setVersion((v) => v + 1);
    } catch (e) {
      setError(friendlyMessage(e));
    } finally {
      setPending(false);
    }
  };

  return (
    <motion.article className="card flex flex-col p-6" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.08 * index, duration: 0.35 }}>
      <header className="flex items-start gap-3">
        <span className="grid h-10 w-10 shrink-0 place-items-center rounded-full bg-raised text-[1.05rem] font-semibold" aria-hidden>{sample.first_name[0]}</span>
        <div className="min-w-0 flex-1">
          <div className="flex items-baseline justify-between gap-2">
            <h3 className="text-[1.15rem] font-semibold">{sample.first_name}</h3>
            {card && <span className="chip shrink-0 !py-0.5">{card.title}</span>}
          </div>
          <div className="truncate text-xs text-muted">{sample.metro_name} &middot; {sample.tier} &middot; {sample.device}</div>
        </div>
      </header>
      <div className="mt-4 rounded-xl border border-alert/25 bg-alert/[0.05] px-4 py-2.5">
        <div className="text-[0.68rem] font-semibold uppercase tracking-[0.12em] text-alert/90">What happened</div>
        <p className="mt-1 text-[0.95rem] leading-snug text-ink/90">{sentence(sample.what_happened)}</p>
      </div>
      <p className="mt-2 px-1 text-xs text-faint">Watches {sample.fav_genre.toLowerCase()} &middot; recently {sample.recent_titles.join(", ")}</p>
      <AnimatePresence mode="wait" initial={false}>
        <motion.div key={version} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={{ duration: 0.25 }} className="flex flex-1 flex-col">
          <div className="mt-4 flex-1 rounded-xl border border-line bg-canvas p-4">
            <div className="text-xs text-faint">Subject</div>
            <div className="mt-0.5 text-[1.02rem] font-semibold leading-snug">{shown.msg.subject}</div>
            <p className="mt-3 text-[0.93rem] leading-relaxed text-ink/85">{shown.msg.body}</p>
            <div className="mt-3 flex items-center gap-2">
              <span className="text-xs text-muted">Watch next</span>
              <span className="inline-flex items-center gap-1.5 rounded-full border border-action/30 bg-action/[0.08] px-2.5 py-1 text-xs text-action">
                <svg viewBox="0 0 12 12" className="h-2.5 w-2.5" fill="currentColor" aria-hidden><path d="M3 1.5v9l7-4.5z" /></svg>
                {shown.msg.watch_next}
              </span>
            </div>
          </div>
          <div className="mt-3 rounded-xl border border-line px-4 py-3">
            <div className="flex items-baseline justify-between">
              <span className="text-[0.68rem] font-semibold uppercase tracking-[0.12em] text-muted">Support agent sees</span>
              {approved && <span className="text-xs text-ok">Queued to support</span>}
            </div>
            <p className="mt-1 text-sm leading-relaxed text-ink/80">{shown.msg.agent_note}</p>
            <ServingLine sampleId={sample.sample_id} />
          </div>
        </motion.div>
      </AnimatePresence>
      <footer className="mt-4 flex items-center justify-between gap-3">
        {shown.live === true ? <span className="text-xs text-ok">Written by AI just now</span>
          : shown.live === false ? <RecordedDot show label="recorded message" />
          : <span className="text-xs text-faint">From the batch run</span>}
        <button className="btn-ghost" onClick={regenerate} disabled={pending} aria-busy={pending}>
          {pending ? (<><span className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-line border-t-ink" /> Writing...</>) : (
            <>
              <svg viewBox="0 0 20 20" className="h-3.5 w-3.5" fill="none" stroke="currentColor" strokeWidth={1.8} aria-hidden>
                <path d="M16 10a6 6 0 1 1-1.8-4.3M16 3.5v3.2h-3.2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              Regenerate
            </>
          )}
        </button>
      </footer>
      {(error || shown.note) && <p className={`mt-2 text-xs ${error ? "text-alert" : "text-faint"}`}>{error ?? shown.note}</p>}
    </motion.article>
  );
}

/** The support agent's live view: this subscriber's row, read from the Lakebase synced tables. Hidden if unreachable. */
function ServingLine({ sampleId }: { sampleId: string }) {
  const q = useQuery({ queryKey: ["serving", sampleId], queryFn: () => api.serving(sampleId), staleTime: 60_000, retry: false });
  const d = q.data;
  if (!d || d.source !== "lakebase" || !d.found || d.churn_prob == null) return null;
  return (
    <p className="mt-2 border-t border-line pt-2 text-xs text-faint" title={`Read from Lakebase table ${d.table}, model ${d.model_version}`}>
      Live from Lakebase &middot; churn score <span className="tnum text-muted">{d.churn_prob.toFixed(2)}</span> &middot; risk
      decile <span className="tnum text-muted">{d.risk_decile}</span> &middot; <span className="tnum">{d.ms}</span> ms
    </p>
  );
}
