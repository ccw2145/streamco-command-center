// Ask display pieces: the lanes, the answer card with its cause bar and document citations, and Genie follow-up cards.
import { useEffect, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { ProvenanceNumber, ResultTable, SqlBlock, useProvenance } from "@/components/Provenance";
import { Check, ConfidenceBadge, ErrorState, RecordedDot } from "@/components/ui";
import { count, durationS, pct } from "@/lib/format";
import type { Answer, Driver, GenieAnswer, LaneDef, LaneDone } from "@/lib/types";

/** A passage retrieved by AI Search (policy section or masked support transcript), copied onto the card by code. */
export interface Citation {
  doc_type: "policy" | "transcript";
  doc_title: string;
  section: string;
  quote: string;
  n_conversations?: number | null;
  tier?: string | null;
}
export type CitedAnswer = Answer & { citations?: Citation[] };

const WORDS = ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight"];
const say = (n: number) => WORDS[n] ?? String(n);

export interface LaneState {
  def: LaneDef;
  status: "waiting" | "running" | "done";
  startedAt?: number;
  finishedAt?: number;
  done?: LaneDone;
}

export function useNow(active: boolean, everyMs = 100): number {
  const [now, setNow] = useState(() => performance.now());
  useEffect(() => {
    if (!active) return;
    const id = setInterval(() => setNow(performance.now()), everyMs);
    return () => clearInterval(id);
  }, [active, everyMs]);
  return now;
}

// ---------------------------------------------------------------- lanes
export function Lanes({ lanes, startedAt, finishedAt }: { lanes: LaneState[]; startedAt: number; finishedAt: number | null }) {
  const now = useNow(finishedAt === null);
  const doneCount = lanes.filter((l) => l.status === "done").length;
  return (
    <section aria-label="Investigation lanes">
      <div className="mb-4 flex items-baseline justify-between">
        <h2 className="text-[1.05rem] font-semibold">{doneCount < lanes.length ? `Checking ${say(lanes.length)} sources at once` : `${cap(say(lanes.length))} sources checked`}</h2>
        <span className="tnum text-sm text-muted">{doneCount}/{lanes.length} &middot; {durationS((finishedAt ?? now) - startedAt)}</span>
      </div>
      <ol className="space-y-2">
        {lanes.map((l, i) => (
          <motion.li key={l.def.lane} initial={{ opacity: 0, x: -12 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: i * 0.07, duration: 0.3 }}>
            <Lane index={i} lane={l} now={now} />
          </motion.li>
        ))}
      </ol>
    </section>
  );
}

const GENIE_STEPS = ["Understanding the question", "Writing SQL on certified metrics", "Running the query", "Answer ready"];
const GENIE_STEP_AT = [0, 2.5, 7];

export function GenieSteps({ elapsedS, finished }: { elapsedS: number; finished: boolean }) {
  let at = GENIE_STEPS.length - 1;
  if (!finished) GENIE_STEP_AT.forEach((t, i) => elapsedS >= t && (at = i));
  if (!finished && at === GENIE_STEPS.length - 1) at = GENIE_STEP_AT.length - 1;
  return (
    <div className="flex items-center gap-2.5 text-sm" aria-label="Genie Agent progress">
      <span className="flex gap-1" aria-hidden>
        {GENIE_STEPS.map((s, i) => (
          <span key={s} className={`h-1.5 w-5 rounded-full transition-colors duration-500 ${
            i < at || finished ? "bg-ok/70" : i === at ? "animate-pulse bg-ink" : "bg-line"}`} />
        ))}
      </span>
      <span className="text-ink/80">{GENIE_STEPS[at]}{!finished && "..."}</span>
    </div>
  );
}

function Lane({ index, lane, now }: { index: number; lane: LaneState; now: number }) {
  const [showSql, setShowSql] = useState(false);
  const { def, status, done } = lane;
  const isGenie = def.lane === "subscribers";
  const elapsed = status === "waiting" || lane.startedAt === undefined ? 0 : (lane.finishedAt ?? now) - lane.startedAt;
  const failed = done && done.status !== "ok";
  return (
    <div className={`card relative overflow-hidden px-5 py-3 ${status === "running" ? "border-faint/60" : ""}`}>
      {status === "running" && (
        <motion.span className="absolute inset-y-0 left-0 w-full bg-gradient-to-r from-transparent via-white/[0.035] to-transparent"
          initial={{ x: "-100%" }} animate={{ x: "100%" }} transition={{ repeat: Infinity, duration: 1.6, ease: "linear" }} aria-hidden />
      )}
      <div className="relative flex items-center gap-3">
        <StatusDot status={status} failed={Boolean(failed)} />
        <span className="tnum text-sm text-faint">{index + 1}</span>
        <span className="font-semibold">{def.label}</span>
        {isGenie && <span className="rounded-full border border-action/40 bg-action/10 px-2 py-0.5 text-[0.68rem] font-semibold text-action">Genie Agent</span>}
        <span className="ml-auto flex items-center gap-3 text-xs text-muted">
          <span className="hidden xl:inline">{def.owner_system}</span>
          <span className="tnum w-11 text-right">{status === "waiting" ? "" : durationS(elapsed)}</span>
        </span>
      </div>
      <div className="relative ml-[3.1rem] mt-1.5 min-h-[1.25rem]">
        {status !== "done" ? (
          isGenie && status === "running" ? (
            <GenieSteps elapsedS={elapsed / 1000} finished={false} />
          ) : (
            <span className="text-sm text-faint">{status === "running" ? `Asking ${def.owner_system}...` : "Queued"}</span>
          )
        ) : (
          <motion.div initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.3 }}>
            <p className={`text-[0.86rem] leading-snug ${failed ? "text-muted" : "text-ink/90"}`}>
              {done?.summary || (failed ? "This system didn't answer; the investigation continued without it." : "")}
            </p>
            {done?.sql_text && (
              <button className="mt-1 text-xs text-muted underline decoration-dotted underline-offset-4 hover:text-ink"
                onClick={() => setShowSql((v) => !v)} aria-expanded={showSql}>
                {showSql ? "Hide SQL" : `Show SQL · ${done.row_count} ${done.row_count === 1 ? "row" : "rows"}`}
              </button>
            )}
            <AnimatePresence>
              {showSql && done?.sql_text && (
                <motion.div initial={{ height: 0, opacity: 0 }} animate={{ height: "auto", opacity: 1 }} exit={{ height: 0, opacity: 0 }} className="overflow-hidden">
                  <SqlBlock sql={done.sql_text} className="mt-2 text-[0.72rem]" />
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        )}
      </div>
    </div>
  );
}

function StatusDot({ status, failed }: { status: LaneState["status"]; failed: boolean }) {
  if (status === "done") {
    return (
      <motion.span initial={{ scale: 0.4 }} animate={{ scale: 1 }} transition={{ type: "spring", stiffness: 500, damping: 22 }}
        className={`grid h-5 w-5 place-items-center rounded-full ${failed ? "bg-raised text-muted" : "bg-ok/15 text-ok"}`}>
        {failed ? <span className="text-xs">–</span> : <Check className="h-3 w-3" />}
      </motion.span>
    );
  }
  if (status === "running") return <span className="h-5 w-5 animate-spin rounded-full border-2 border-line border-t-ink" aria-label="running" />;
  return <span className="h-5 w-5 rounded-full border-2 border-line" aria-label="waiting" />;
}

// ---------------------------------------------------------------- answer
const STYLE: Record<Driver["driver"], { fill: string; text: string }> = {
  playback: { fill: "#EEF1F7", text: "#0A0D14" },
  payment: { fill: "#A9BCE3", text: "#0A0D14" },
  event_only: { fill: "#4A5670", text: "#EEF1F7" },
  unexplained: { fill: "#262E40", text: "#9AA3B5" },
};

function CauseBar({ drivers }: { drivers: Driver[] }) {
  const { open } = useProvenance();
  const total = drivers.reduce((s, d) => s + d.pct, 0) || 1;
  return (
    <div className="flex h-16 w-full gap-1 overflow-hidden rounded-xl" role="list" aria-label="Why people cancelled">
      {drivers.map((d, i) => (
        <motion.button role="listitem" key={d.driver} type="button" onClick={() => open(d.metric_id)}
          title={`${d.label}: ${pct(d.pct)}. Click to see the SQL.`}
          className="flex items-center overflow-hidden px-4 text-left transition-[filter] hover:brightness-110"
          style={{ background: STYLE[d.driver]?.fill ?? "#262E40", color: STYLE[d.driver]?.text ?? "#EEF1F7" }}
          initial={{ flexGrow: 0.0001, opacity: 0 }} animate={{ flexGrow: d.pct / total, opacity: 1 }}
          transition={{ delay: 0.15 + i * 0.12, duration: 0.7, ease: [0.2, 0.7, 0.2, 1] }}>
          <span className="tnum text-[1.65rem] font-semibold tracking-tight">{pct(d.pct)}</span>
        </motion.button>
      ))}
    </div>
  );
}

const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

function Citations({ citations }: { citations: Citation[] }) {
  return (
    <section className="mt-6" aria-label="From your documents">
      <h3 className="font-semibold">From your documents</h3>
      <ul className="mt-3 space-y-3">
        {citations.map((c) => (
          <li key={`${c.doc_title}-${c.section}`} className="rounded-xl border border-line bg-raised/40 px-4 py-3">
            <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
              <span className="rounded-full border border-faint/50 px-2 py-0.5 text-[0.66rem] font-semibold uppercase tracking-[0.1em] text-muted">
                {c.doc_type === "policy" ? "Policy" : "Support chats"}
              </span>
              <span className="font-medium">{c.doc_type === "policy" ? c.doc_title : `${c.tier ?? ""} support conversations`.trim()}</span>
              <span className="text-sm text-muted">&middot; {c.section}</span>
              {c.doc_type === "transcript" && typeof c.n_conversations === "number" && (
                <span className="tnum ml-auto text-sm font-semibold text-ink">{count(c.n_conversations)} conversations</span>
              )}
            </div>
            <blockquote className="mt-1.5 border-l-2 border-faint/60 pl-3 text-[0.95rem] leading-snug text-ink/85">
              &ldquo;{c.quote}&rdquo;
            </blockquote>
          </li>
        ))}
      </ul>
    </section>
  );
}

export function AnswerCard({ answer, recorded, onAsk, primary }: { answer: CitedAnswer; recorded: boolean; onAsk: (q: string) => void; primary: string }) {
  if (answer.declined || !answer.total) {
    return (
      <motion.article className="card p-7" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
        <div className="eyebrow">No confident answer</div>
        <h2 className="mt-3 text-[1.6rem] font-semibold leading-snug">{answer.headline || "I won't guess on this one."}</h2>
        <p className="mt-2 text-muted">{answer.narrative}</p>
        <button className="chip mt-5 text-ink hover:border-faint" onClick={() => onAsk(primary)}>{primary}</button>
      </motion.article>
    );
  }
  const { total, reconciliation: rec } = answer;
  return (
    <motion.article className="card p-7" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.45 }} aria-label="Answer">
      <div className="flex items-center justify-between">
        <div className="eyebrow">The answer</div>
        <RecordedDot show={recorded} label="recorded run" />
      </div>
      <h2 className="mt-3 text-headline">{answer.headline}</h2>
      <div className="mt-6 grid grid-cols-3 gap-4 border-y border-line py-5">
        <Figure label="Cancellations"><ProvenanceNumber metricId="cancels_pct_over">{compactK(total.observed)}</ProvenanceNumber></Figure>
        <Figure label={`Expected (normal range ${count(total.band_lo)}–${count(total.band_hi)})`}>
          <ProvenanceNumber metricId="cancels_pct_over">{count(total.expected)}</ProvenanceNumber>
        </Figure>
        <Figure label={`Above expected (${count(total.excess)} extra)`} alert>
          <ProvenanceNumber metricId="cancels_pct_over">+{pct(total.pct_over)}</ProvenanceNumber>
        </Figure>
      </div>
      <div className="mt-6">
        <div className="mb-3 flex items-baseline justify-between">
          <h3 className="font-semibold">Why people cancelled</h3>
          <span className="text-xs text-muted">Share of the {count(total.excess)} extra cancellations. Click any % for its SQL.</span>
        </div>
        <CauseBar drivers={answer.drivers} />
        <div className="mt-4 text-right text-[0.68rem] font-semibold uppercase tracking-[0.12em] text-faint">How sure are we</div>
        <ul className="mt-1.5 space-y-3.5">
          {answer.drivers.map((d) => (
            <li key={d.driver} className="grid grid-cols-[14px_4.2rem_1fr_auto] items-start gap-x-3">
              <span className="mt-1.5 h-3 w-3 rounded-sm" style={{ background: STYLE[d.driver]?.fill }} aria-hidden />
              <ProvenanceNumber metricId={d.metric_id} className="tnum text-[1.15rem] font-semibold">{pct(d.pct)}</ProvenanceNumber>
              <div>
                <div className="font-medium">{d.label} <span className="tnum font-normal text-muted">&middot; {count(d.n_cancels)}</span></div>
                <p className="mt-0.5 text-sm leading-snug text-muted">{d.evidence}</p>
              </div>
              <ConfidenceBadge confidence={d.confidence} evidenceType={d.evidence_type} />
            </li>
          ))}
        </ul>
      </div>
      {answer.citations && answer.citations.length > 0 && <Citations citations={answer.citations} />}
      {rec && (
        <div className={`mt-6 rounded-xl border p-4 ${rec.status !== "sources_disagree" ? "border-ok/30 bg-ok/[0.05]" : "border-alert/40 bg-alert/[0.06]"}`}>
          <div className="flex items-center gap-2">
            <span className="font-semibold">{rec.status !== "sources_disagree" ? "The numbers reconcile" : "Sources disagree"}</span>
          </div>
          <p className="mt-1.5 text-sm text-muted">
            Subscriber system <span className="tnum text-ink">{count(rec.subscriber_live)}</span>, Finance ledger{" "}
            <span className="tnum text-ink">{count(rec.ledger)}</span>:{" "}
            <ProvenanceNumber metricId="recon_sports" className="tnum text-ink">{pct(rec.pct_diff)}</ProvenanceNumber> apart.
          </p>
        </div>
      )}
      {answer.narrative && (
        <details className="group mt-5">
          <summary className="cursor-pointer list-none text-sm text-muted transition hover:text-ink">
            <span className="group-open:hidden">Read the full explanation &darr;</span>
            <span className="hidden group-open:inline">Hide explanation &uarr;</span>
          </summary>
          <p className="mt-3 max-w-3xl text-[0.98rem] leading-relaxed text-ink/85">{answer.narrative}</p>
        </details>
      )}
    </motion.article>
  );
}

function compactK(n: number): string {
  return n >= 10_000 ? `${(n / 1000).toFixed(1).replace(/\.0$/, "")}k` : count(n);
}

function Figure({ label, children, alert }: { label: string; children: ReactNode; alert?: boolean }) {
  return (
    <div>
      <div className={`tnum text-num-sm ${alert ? "text-alert" : ""}`}>{children}</div>
      <div className="mt-1.5 text-xs text-muted">{label}</div>
    </div>
  );
}

export function PendingAnswer({ allDone }: { allDone: boolean }) {
  return (
    <div className="card p-7" aria-live="polite">
      <div className="eyebrow">The answer</div>
      <p className="mt-3 text-[1.05rem] text-muted">
        {allDone ? "Writing the answer from what every source returned..." : "Waiting for every source to report back..."}
      </p>
      <div className="mt-6 space-y-3">
        <div className="skeleton h-8 w-[85%]" />
        <div className="skeleton h-8 w-[60%]" />
      </div>
      <div className="mt-7 grid grid-cols-3 gap-4">
        <div className="skeleton h-16" /><div className="skeleton h-16" /><div className="skeleton h-16" />
      </div>
      <p className="mt-5 text-xs text-faint">Numbers come only from what the tools returned; the AI only writes the sentences.</p>
    </div>
  );
}

// ---------------------------------------------------------------- follow-ups
export interface FollowupItem {
  id: number;
  question: string;
  startedAt: number;
  answer?: GenieAnswer;
  error?: unknown;
}

export function GenieCard({ item, onRetry }: { item: FollowupItem; onRetry: () => void }) {
  const pending = !item.answer && item.error === undefined;
  const now = useNow(pending, 150);
  const [showSql, setShowSql] = useState(false);
  const a = item.answer;
  return (
    <motion.article layout initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="card p-6" aria-label={`Follow-up: ${item.question}`}>
      <div className="flex items-center gap-3">
        <span className="rounded-full border border-action/40 bg-action/10 px-2 py-0.5 text-[0.68rem] font-semibold text-action">Genie Agent</span>
        <span className="font-medium">{item.question}</span>
        {a && <RecordedDot show={a.recorded} className="ml-auto" />}
      </div>
      {pending && (
        <div className="mt-4">
          <GenieSteps elapsedS={(now - item.startedAt) / 1000} finished={false} />
          <div className="mt-4 space-y-2"><div className="skeleton h-4 w-3/4" /><div className="skeleton h-4 w-1/2" /></div>
        </div>
      )}
      {item.error !== undefined && <div className="mt-4"><ErrorState compact error={item.error} onRetry={onRetry} /></div>}
      {a && (
        <div className="mt-4">
          {a.declined && (
            <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-alert/40 bg-alert/[0.08] px-3 py-1 text-sm font-medium text-alert">
              <span className="font-bold">!</span> Sources disagree: no confident number
            </div>
          )}
          <p className="max-w-4xl whitespace-pre-line text-[1.05rem] leading-relaxed text-ink/90">{a.text}</p>
          <Bars a={a} />
          <div className="mt-4 flex items-center gap-4 text-xs text-muted">
            {a.sql && (
              <button className="underline decoration-dotted underline-offset-4 hover:text-ink" onClick={() => setShowSql((v) => !v)}>
                {showSql ? "Hide SQL" : "Show SQL"}
              </button>
            )}
            {a.duration_ms > 0 && <span className="tnum">Answered in {durationS(a.duration_ms)}</span>}
          </div>
          {showSql && a.sql && <SqlBlock sql={a.sql} className="mt-3" />}
        </div>
      )}
    </motion.article>
  );
}

/** Two-column (label, number) results with several rows read better as bars; anything else is a table. */
function Bars({ a }: { a: GenieAnswer }) {
  const [all, setAll] = useState(false);
  const isBars = a.columns.length === 2 && a.rows.length >= 3 && a.rows.every((r) => typeof r[0] === "string" && typeof r[1] === "number");
  if (!isBars) return a.columns.length > 0 ? <ResultTable columns={a.columns} rows={a.rows} className="mt-4" maxRows={10} /> : null;
  const bars = a.rows.map((r) => ({ label: r[0] as string, value: r[1] as number }));
  const max = Math.max(1, ...bars.map((b) => b.value));
  const shown = all ? bars : bars.slice(0, 8);
  return (
    <div className="mt-5">
      <ul className="grid gap-y-2">
        {shown.map((b, i) => (
          <li key={b.label} className="grid grid-cols-[10rem_1fr_3.5rem] items-center gap-3 text-sm">
            <span className={i === 0 ? "font-semibold" : "text-muted"}>{b.label}</span>
            <span className="h-2.5 overflow-hidden rounded-full bg-raised">
              <motion.span className={`block h-full rounded-full ${i === 0 ? "bg-ink" : "bg-faint"}`} initial={{ width: 0 }}
                animate={{ width: `${(b.value / max) * 100}%` }} transition={{ duration: 0.6, delay: 0.03 * i }} />
            </span>
            <span className="tnum text-right">{count(b.value)}</span>
          </li>
        ))}
      </ul>
      {bars.length > 8 && (
        <button className="mt-2 text-xs text-faint hover:text-ink" onClick={() => setAll((v) => !v)}>
          {all ? "Show fewer" : `Show all ${bars.length}`}
        </button>
      )}
    </div>
  );
}
