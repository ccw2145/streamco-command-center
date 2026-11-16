// Click any number: its SQL, source tables and the recorded result.
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useQuery } from "@tanstack/react-query";
import { api, friendlyMessage } from "@/lib/api";
import { clockLabel } from "@/lib/format";
import type { Lineage } from "@/lib/types";

const Ctx = createContext<{ open: (metricId: string) => void }>({ open: () => {} });
export const useProvenance = () => useContext(Ctx);

export function ProvenanceProvider({ children }: { children: ReactNode }) {
  const [target, setTarget] = useState<string | null>(null);
  const open = useCallback((id: string) => setTarget(id), []);
  const close = useCallback(() => setTarget(null), []);
  const value = useMemo(() => ({ open }), [open]);
  useEffect(() => {
    if (!target) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && close();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [target, close]);
  return (
    <Ctx.Provider value={value}>
      {children}
      <AnimatePresence>{target && <Dialog key={target} metricId={target} onClose={close} />}</AnimatePresence>
    </Ctx.Provider>
  );
}

/** A number that opens its provenance when clicked. */
export function ProvenanceNumber({ metricId, children, className = "" }: { metricId: string; children: ReactNode; className?: string }) {
  const { open } = useProvenance();
  return (
    <button
      type="button"
      className={`num-link text-left ${className}`}
      onClick={(e) => {
        e.stopPropagation();
        open(metricId);
      }}
      title="Where does this number come from?"
    >
      {children}
    </button>
  );
}

function Dialog({ metricId, onClose }: { metricId: string; onClose: () => void }) {
  const q = useQuery({ queryKey: ["provenance", metricId], queryFn: () => api.provenance(metricId) });
  return (
    <motion.div
      className="fixed inset-0 z-50 grid place-items-center bg-black/60 p-6 backdrop-blur-[2px]"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      onClick={onClose}
    >
      <motion.div
        role="dialog"
        aria-modal="true"
        aria-label="Where this number comes from"
        className="card scroll-thin max-h-[86vh] w-full max-w-3xl overflow-y-auto bg-raised p-7"
        initial={{ y: 12, scale: 0.985 }}
        animate={{ y: 0, scale: 1 }}
        exit={{ y: 8, opacity: 0 }}
        transition={{ type: "spring", stiffness: 420, damping: 34 }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-6">
          <div>
            <div className="eyebrow">Where this number comes from</div>
            <h3 className="mt-2 text-xl font-semibold">{q.data?.label ?? "Loading..."}</h3>
          </div>
          <button className="btn-ghost" onClick={onClose}>Close</button>
        </div>
        {q.isError && <p className="mt-6 text-sm text-muted">{friendlyMessage(q.error)}</p>}
        {q.data && (
          <>
            <div className="mt-5 flex flex-wrap items-center gap-2 text-sm">
              <span className="text-muted">System of record</span>
              <span className="chip text-ink">{q.data.owner_system}</span>
              <span className="ml-3 text-muted">Tables</span>
              {q.data.tables.map((t) => (
                <span key={t} className="chip font-mono text-[0.72rem]">{t}</span>
              ))}
            </div>
            {q.data.lineage && <LineagePath lineage={q.data.lineage} />}
            <SqlBlock sql={q.data.sql} className="mt-5" />
            <div className="mt-5 text-xs text-muted">Result, recorded {clockLabel(q.data.ran_at)}</div>
            <ResultTable columns={q.data.columns} rows={q.data.rows} className="mt-2" />
          </>
        )}
      </motion.div>
    </motion.div>
  );
}

/** Upstream path of a governed number: source -> bronze -> silver -> gold -> metric view. */
function LineagePath({ lineage }: { lineage: Lineage }) {
  return (
    <section className="mt-5" aria-label="Lineage">
      <div className="flex items-baseline justify-between gap-4">
        <span className="text-sm text-muted">Lineage</span>
        {lineage.url && (
          <a href={lineage.url} target="_blank" rel="noreferrer" className="text-xs text-action hover:underline">
            Open in Catalog Explorer &rarr;
          </a>
        )}
      </div>
      <ol className="mt-2 flex items-stretch gap-1.5">
        {lineage.hops.map((h, i) => (
          <li key={h.layer} className="flex min-w-0 flex-1 items-stretch gap-1.5">
            {i > 0 && <span className="self-center text-faint" aria-hidden>&rarr;</span>}
            <div className={`min-w-0 flex-1 rounded-lg border px-2.5 py-2 ${i === lineage.hops.length - 1 ? "border-action/40 bg-action/[0.06]" : "border-line bg-canvas"}`}
              title={`${h.tables.join(", ")} (from ${h.basis})`}>
              <div className="text-xs font-medium">{h.layer}</div>
              <ul className="mt-1 space-y-0.5">
                {h.tables.map((t) => (
                  <li key={t} className={`break-words leading-tight text-muted ${i === 0 ? "text-[0.7rem]" : "font-mono text-[0.62rem]"}`}>
                    {t.replace(/^(bronze|silver|gold)_/, "").replace(/_/g, "_\u200b")}
                  </li>
                ))}
              </ul>
            </div>
          </li>
        ))}
      </ol>
      <p className="mt-1.5 text-[0.7rem] text-faint">
        From {lineage.basis}; the source hop is from the pipeline definition (Auto Loader over the landing volume).
      </p>
    </section>
  );
}

const KEYWORDS =
  "SELECT|FROM|WHERE|AND|OR|GROUP BY|ORDER BY|AS|DESC|ASC|MEASURE|SUM|COUNT|AVG|MAX|MAX_BY|JOIN|ON|LIMIT|DATE|TIMESTAMP|ALL|CASE|WHEN|THEN|ELSE|END|IN|NOT|NULL|IF|BETWEEN|IS";
const SQL_SPLIT = new RegExp(`\\b(${KEYWORDS})\\b`);
const SQL_IS_KW = new RegExp(`^(?:${KEYWORDS})$`);

export function SqlBlock({ sql, className = "" }: { sql: string; className?: string }) {
  const pretty = sql
    .replace(/\s+(FROM|WHERE|GROUP BY|ORDER BY|LIMIT)\s+/g, "\n$1 ")
    .replace(/\s+AND\s+/g, "\n  AND ");
  return (
    <pre className={`scroll-thin overflow-x-auto rounded-xl border border-line bg-canvas p-4 font-mono text-[0.8rem] leading-relaxed text-ink/90 ${className}`}>
      {pretty.split(SQL_SPLIT).map((p, i) =>
        SQL_IS_KW.test(p) ? <span key={i} className="text-action">{p}</span> : <span key={i}>{p}</span>,
      )}
    </pre>
  );
}

export function ResultTable({ columns, rows, className = "", maxRows = 20 }: {
  columns: string[];
  rows: unknown[][];
  className?: string;
  maxRows?: number;
}) {
  if (!columns.length) return null;
  return (
    <div className={`scroll-thin overflow-x-auto rounded-xl border border-line ${className}`}>
      <table className="w-full text-sm">
        <thead className="bg-surface text-left text-xs text-muted">
          <tr>{columns.map((c) => <th key={c} className="px-3 py-2 font-medium">{c}</th>)}</tr>
        </thead>
        <tbody className="tnum">
          {rows.slice(0, maxRows).map((r, i) => (
            <tr key={i} className="border-t border-line">
              {r.map((v, j) => (
                <td key={j} className={`px-3 py-2 ${typeof v === "number" ? "text-right" : ""}`}>{cell(v)}</td>
              ))}
            </tr>
          ))}
          {!rows.length && (
            <tr><td className="px-3 py-3 text-muted" colSpan={columns.length}>No rows returned.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function cell(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return Number.isInteger(v) ? v.toLocaleString("en-US") : String(Math.round(v * 10000) / 10000);
  if (Array.isArray(v)) return v.join(", ");
  return String(v);
}
