// One Truth: five source systems -> one metric view; three teams, three numbers -> one governed number; four tiles.
import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import { useScreen } from "@/lib/useScreen";
import { HowItWorksDrawer, Section } from "@/components/Drawer";
import { ProvenanceNumber } from "@/components/Provenance";
import { useShell } from "@/components/Shell";
import { Check, ErrorState, ScreenSkeleton, StatTile } from "@/components/ui";
import { compact, count } from "@/lib/format";
import type { TruthPayload } from "@/lib/types";

export function OneTruth() {
  const q = useScreen("truth");
  if (q.isPending) return <ScreenSkeleton tiles={2} />;
  if (q.isError) return <ErrorState error={q.error} onRetry={() => q.refetch()} />;
  const d = q.data;
  return (
    <div>
      <header className="flex items-end justify-between gap-10">
        <div className="max-w-5xl">
          <div className="eyebrow">1 of 4 &middot; The morning after the Big Game &middot; What happened?</div>
          <h1 className="mt-3 text-[2.4rem] font-semibold leading-[1.1] tracking-tight">One number every team agrees on</h1>
          <motion.p className="mt-3 max-w-4xl text-[1.1rem] leading-relaxed text-muted" initial={{ opacity: 0 }}
            animate={{ opacity: 1 }} transition={{ delay: 0.3, duration: 0.5 }}>
            {d.story}
          </motion.p>
        </div>
        <Badges badges={d.badges} />
      </header>

      <div className="mt-7 grid grid-cols-12 gap-5">
        <div className="col-span-7"><SourceMap d={d} /></div>
        <div className="col-span-5"><TeamNumbers tc={d.team_counts} /></div>
      </div>

      <div className="mt-5 grid grid-cols-4 gap-5">
        {d.tiles.map((t, i) => (
          <div key={t.id} className="flex flex-col">
            <StatTile index={i} label={t.label} display={t.display} compareLabel={t.compare_label}
              compareDisplay={t.compare_display} status={t.status} metricId={t.metric_id} />
            <div className="flex h-7 items-end justify-end px-2">
              {t.next && <Link to={t.next.to} className="text-sm text-muted transition hover:text-ink">{t.next.label} &rarr;</Link>}
            </div>
          </div>
        ))}
      </div>

      <HowItWorksDrawer hiw={d.how_it_works}>
        <Section title="Where each source lands">
          <table className="w-full text-sm">
            <thead className="text-left text-xs text-muted">
              <tr><th className="py-1.5 font-medium">Source</th><th className="py-1.5 font-medium">Bronze tables</th>
                <th className="py-1.5 font-medium">In production</th></tr>
            </thead>
            <tbody>
              {d.sources.map((s) => (
                <tr key={s.source_id} className="border-t border-line align-top">
                  <td className="py-2 pr-3">{s.system_type}</td>
                  <td className="py-2 pr-3 font-mono text-xs text-muted">{s.bronze_tables.join(", ")}</td>
                  <td className="py-2 text-muted">{s.prod_connector}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>
      </HowItWorksDrawer>
    </div>
  );
}

function Badges({ badges }: { badges: TruthPayload["badges"] }) {
  const { setGatewayOpen } = useShell();
  return (
    <div className="flex shrink-0 flex-col items-end gap-2 pb-1">
      {badges.map((b) => (
        <button key={b.id} type="button" onClick={() => setGatewayOpen(true)} title={`${b.detail} Click for the governance detail.`}
          className="inline-flex items-center gap-1.5 rounded-full border border-ok/40 bg-ok/10 px-3 py-1 text-sm text-ok transition hover:border-ok">
          <Check className="h-3.5 w-3.5" />
          {b.label}
        </button>
      ))}
    </div>
  );
}

function SourceMap({ d }: { d: TruthPayload }) {
  const { sources, pipeline, core_entity: entity, data_quality: dq } = d;
  return (
    <section className="card flex h-full flex-col p-6" aria-label="Source map">
      <div className="flex items-baseline justify-between gap-4">
        <h2 className="text-[1.05rem] font-semibold">Five systems, one governed copy</h2>
        <span className="text-xs text-faint">Simulated sources &middot; production connector on each card</span>
      </div>
      {entity && (
        <p className="mt-1 text-xs text-muted">
          <span className="text-ink/90">Core entity: the {entity.name}</span> &middot;{" "}
          <ProvenanceNumber metricId={entity.metric_id} className="text-muted">{entity.label}</ProvenanceNumber>
        </p>
      )}
      <div className="mt-5 grid grid-cols-[minmax(0,1.9fr)_2rem_minmax(0,0.8fr)_1.75rem_minmax(0,0.8fr)] items-stretch">
        <ol className="space-y-2">
          {sources.map((s, i) => (
            <motion.li key={s.source_id} initial={{ opacity: 0, x: -8 }} animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.05 * i, duration: 0.3 }} className="rounded-xl border border-line bg-raised px-3.5 py-2">
              <div className="flex items-baseline justify-between gap-3">
                <span className="truncate text-[0.95rem] font-semibold">{s.system_type}</span>
                <span className="tnum shrink-0 text-xs text-muted">
                  <ProvenanceNumber metricId="source_systems" className="text-ink">{compact(s.row_count)}</ProvenanceNumber> rows
                </span>
              </div>
              <div className="truncate text-xs text-muted">{s.name}</div>
              <div className="truncate text-[0.7rem] text-faint">in production: <span className="text-muted">{s.prod_connector}</span></div>
            </motion.li>
          ))}
        </ol>
        <Converge n={sources.length} />
        <div className="flex flex-col justify-center rounded-xl border border-line p-3">
          <div className="eyebrow mb-2 px-1">Lakeflow pipelines</div>
          <div className="space-y-1.5">
            {pipeline.layers.map((l) => (
              <div key={l.id} className="rounded-lg bg-raised px-3 py-2">
                <div className="font-medium">{l.label}</div>
                <div className="text-xs text-muted">{l.summary}</div>
              </div>
            ))}
          </div>
          <div className="mt-2 px-1 text-[0.7rem] text-faint">Unity Catalog</div>
        </div>
        <div className="flex items-center justify-center text-faint" aria-hidden>
          <svg viewBox="0 0 24 12" className="h-3 w-6"><path d="M0 6h21m-5-5 5 5-5 5" fill="none" stroke="currentColor" strokeWidth="1.6" /></svg>
        </div>
        <div className="flex flex-col justify-center rounded-xl border border-action/40 bg-action/[0.06] p-4">
          <div className="eyebrow !text-action">One metric view</div>
          <div className="mt-2 font-mono text-[0.76rem] leading-snug text-ink">{pipeline.metric_view.name.replace(/_/g, "_​")}</div>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {pipeline.metric_view.measures.map((m) => (
              <span key={m} className="rounded-md bg-canvas/60 px-1.5 py-0.5 font-mono text-[0.7rem] text-muted">{m}</span>
            ))}
          </div>
          <p className="mt-3 text-xs leading-snug text-muted">Every screen, Genie and the agent read this definition.</p>
        </div>
      </div>
      {dq && <QualityLine dq={dq} />}
    </section>
  );
}

/** One line: the pipelines' expectation results from the event log. A failing rule is shown, not hidden. */
function QualityLine({ dq }: { dq: NonNullable<TruthPayload["data_quality"]> }) {
  const warn = dq.status !== "ok";
  return (
    <div className="mt-auto pt-3">
      <ProvenanceNumber metricId={dq.metric_id} className="!no-underline flex w-full items-center gap-2 border-t border-line pt-2.5 text-xs">
        <span className={`inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full ${warn ? "bg-raised text-muted" : "bg-ok/15 text-ok"}`}>
          {warn ? <span className="text-[0.65rem] font-bold">!</span> : <Check className="h-2.5 w-2.5" />}
        </span>
        <span className="shrink-0 whitespace-nowrap text-muted" title={`Lakeflow expectations, latest run of ${dq.pipelines.join(" and ")}`}>Data quality:</span>
        <span className="shrink-0 whitespace-nowrap text-ink/90 underline decoration-line underline-offset-4">{dq.label}</span>
        {warn && (
          <span className="min-w-0 truncate text-faint" title={dq.detail}>
            failing: {dq.failing.map((f) => f.rule).join(", ")}
          </span>
        )}
      </ProvenanceNumber>
    </div>
  );
}

function Converge({ n }: { n: number }) {
  const ys = Array.from({ length: n }, (_, i) => ((i + 0.5) / n) * 100);
  return (
    <svg viewBox="0 0 36 100" preserveAspectRatio="none" className="h-full w-full text-line" aria-hidden>
      {ys.map((y) => (
        <path key={y} d={`M0 ${y} C 18 ${y}, 18 50, 36 50`} fill="none" stroke="currentColor" strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
      ))}
    </svg>
  );
}

function TeamNumbers({ tc }: { tc: TruthPayload["team_counts"] }) {
  const ok = tc.reconciliation.status !== "sources_disagree";
  return (
    <section className="card flex h-full flex-col p-6" aria-label="Three teams, three numbers">
      <h2 className="text-[1.05rem] font-semibold">Three teams, three numbers</h2>
      <p className="mt-1 text-sm text-muted">Yesterday&rsquo;s {tc.tier} cancellations as each team counts them, and why each differs</p>
      <ul className="mt-4 space-y-2">
        {tc.teams.map((t, i) => (
          <motion.li key={t.team} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.08 * i }}
            className="grid grid-cols-[5.5rem_6rem_1fr] items-baseline gap-3 rounded-xl border border-line px-4 py-2.5">
            <span className="text-sm font-medium">{t.short}</span>
            <span className="flex flex-col">
              <ProvenanceNumber metricId={t.metric_id} className="tnum text-[1.45rem] font-semibold text-muted">{count(t.count)}</ProvenanceNumber>
              {t.delta != null && t.delta !== 0 && (
                <span className="tnum text-xs text-faint">{t.delta > 0 ? "+" : "\u2212"}{count(Math.abs(t.delta))} vs governed</span>
              )}
            </span>
            <span className="text-xs leading-snug text-faint">{t.bridge ?? t.definition}</span>
          </motion.li>
        ))}
      </ul>
      <div className="my-3 flex items-center gap-3 text-xs text-faint" aria-hidden>
        <span className="h-px flex-1 bg-line" />
        <svg viewBox="0 0 12 16" className="h-4 w-3"><path d="M6 0v13m-4.5-4.5L6 13l4.5-4.5" fill="none" stroke="currentColor" strokeWidth="1.5" /></svg>
        <span className="h-px flex-1 bg-line" />
      </div>
      <motion.div initial={{ opacity: 0, scale: 0.98 }} animate={{ opacity: 1, scale: 1 }} transition={{ delay: 0.35, duration: 0.35 }}
        className="rounded-xl border border-action/40 bg-action/[0.06] px-5 py-4">
        <div className="eyebrow !text-action">One governed definition</div>
        <div className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-2">
          <ProvenanceNumber metricId={tc.governed.metric_id} className="tnum text-num-lg">{count(tc.governed.count)}</ProvenanceNumber>
          <ProvenanceNumber metricId="recon_sports" className="!no-underline">
            <span className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-sm ${
              ok ? "border-ok/40 bg-ok/10 text-ok" : "border-alert/40 bg-alert/10 text-alert"}`}>
              {ok ? <Check className="h-3.5 w-3.5" /> : <span className="font-bold">!</span>}
              {tc.reconciliation.label}
            </span>
          </ProvenanceNumber>
        </div>
        <p className="mt-2 text-sm leading-snug text-muted">{tc.governed.definition} Each team&rsquo;s count reconciles to it.</p>
      </motion.div>
    </section>
  );
}
