// Small shared pieces: icons, skeletons, error state, the stat tile, the approve button, the recorded dot.
import { useState, type ReactNode } from "react";
import { motion } from "framer-motion";
import { friendlyMessage } from "@/lib/api";
import type { Driver, Status3 } from "@/lib/types";
import { useProvenance } from "./Provenance";

export function Check({ className = "h-4 w-4" }: { className?: string }) {
  return (
    <svg viewBox="0 0 20 20" className={className} fill="none" stroke="currentColor" strokeWidth={2.4} aria-hidden>
      <path d="M4.5 10.5l3.5 3.5 7.5-8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export function Skeleton({ className = "" }: { className?: string }) {
  return <div aria-hidden className={`skeleton ${className}`} />;
}

export function ScreenSkeleton({ tiles = 3 }: { tiles?: number }) {
  return (
    <div className="space-y-8" role="status" aria-label="Loading">
      <div className="space-y-3">
        <Skeleton className="h-4 w-40" />
        <Skeleton className="h-10 w-[46%]" />
      </div>
      <div className="grid gap-5" style={{ gridTemplateColumns: `repeat(${tiles}, minmax(0, 1fr))` }}>
        {Array.from({ length: tiles }).map((_, i) => (
          <Skeleton key={i} className="h-40" />
        ))}
      </div>
      <Skeleton className="h-80" />
    </div>
  );
}

export function ErrorState({ error, title = "This view is taking a breather", onRetry, compact }: {
  error?: unknown;
  title?: string;
  onRetry?: () => void;
  compact?: boolean;
}) {
  const msg = friendlyMessage(error);
  if (compact) {
    return (
      <div className="flex items-center justify-between gap-4 rounded-xl border border-line bg-raised px-4 py-3 text-sm text-muted">
        <span>{msg}</span>
        {onRetry && <button className="btn-ghost shrink-0" onClick={onRetry}>Try again</button>}
      </div>
    );
  }
  return (
    <div className="card mx-auto mt-16 max-w-xl p-10 text-center" role="alert">
      <h2 className="text-xl font-semibold">{title}</h2>
      <p className="mt-3 text-muted">{msg}</p>
      {onRetry && <button className="btn-ghost mt-6" onClick={onRetry}>Try again</button>}
    </div>
  );
}

/** The small grey dot that says this came from the recorded copy, not a live call. */
export function RecordedDot({ show, label = "recorded", className = "" }: { show: boolean; label?: string; className?: string }) {
  if (!show) return null;
  return (
    <span className={`inline-flex items-center gap-1.5 text-[0.7rem] text-faint ${className}`} title="Served from the recorded copy">
      <span className="h-1.5 w-1.5 rounded-full bg-faint" aria-hidden />
      {label}
    </span>
  );
}

export function StatTile({ label, display, compareLabel, compareDisplay, status = "ok", metricId, index = 0 }: {
  label: string;
  display: string;
  compareLabel?: string;
  compareDisplay?: string;
  status?: Status3;
  metricId?: string;
  index?: number;
}) {
  const { open } = useProvenance();
  const isAlert = status === "alert";
  return (
    <motion.button
      type="button"
      disabled={!metricId}
      onClick={() => metricId && open(metricId)}
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.05 * index, duration: 0.35, ease: "easeOut" }}
      className="card group relative flex h-full flex-col justify-between overflow-hidden p-6 text-left transition hover:border-faint"
      title="Click to see where this number comes from"
    >
      {isAlert && <span className="absolute inset-x-0 top-0 h-[3px] bg-alert" aria-hidden />}
      <div className="flex items-center justify-between">
        <span className="eyebrow">{label}</span>
        {isAlert && <span className="h-2 w-2 rounded-full bg-alert" aria-label="alert" />}
      </div>
      <div className={`tnum mt-4 text-num-lg ${isAlert ? "text-alert" : "text-ink"}`}>{display}</div>
      {compareLabel && (
        <div className="mt-auto flex flex-wrap items-baseline gap-x-2 pt-5 text-[0.95rem]">
          <span className="text-muted">{compareLabel}</span>
          <span className="tnum font-medium text-ink/90">{compareDisplay}</span>
        </div>
      )}
      <span className="absolute bottom-4 right-5 text-xs text-faint opacity-0 transition group-hover:opacity-100">Show source</span>
    </motion.button>
  );
}

/** Safe to double-click: ignores clicks while pending and once approved (the backend is idempotent too). */
export function ApproveButton({ approved, onApprove, hint }: { approved: boolean; onApprove: () => Promise<unknown>; hint: string }) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const click = async () => {
    if (pending || approved) return;
    setPending(true);
    setError(null);
    try {
      await onApprove();
    } catch (e) {
      setError(friendlyMessage(e));
    } finally {
      setPending(false);
    }
  };
  return (
    <div className="flex flex-col items-end gap-2">
      {approved ? (
        <motion.div
          initial={{ scale: 0.96, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          className="inline-flex items-center gap-2.5 rounded-xl border border-ok/40 bg-ok/10 px-7 py-4 text-base font-semibold text-ok"
          role="status"
        >
          <Check /> Plan approved
        </motion.div>
      ) : (
        <button className="btn-primary px-7 py-4 text-base" onClick={click} disabled={pending} aria-busy={pending}>
          {pending ? (
            <>
              <span className="h-4 w-4 animate-spin rounded-full border-2 border-canvas/30 border-t-canvas" /> Approving...
            </>
          ) : (
            "Approve plan"
          )}
        </button>
      )}
      {error ? <span className="text-sm text-alert" role="alert">{error}</span> : !approved && <span className="text-xs text-muted">{hint}</span>}
    </div>
  );
}

const EVIDENCE: Record<Driver["evidence_type"], string> = {
  observed: "Seen directly",
  inferred: "Strong link",
  expected: "Expected",
  unexplained: "No signal",
};
const CONF: Record<Driver["confidence"], { label: string; dots: number }> = {
  high: { label: "High", dots: 3 },
  medium: { label: "Medium", dots: 2 },
  low: { label: "Low", dots: 1 },
};

export function ConfidenceBadge({ confidence, evidenceType }: { confidence: Driver["confidence"]; evidenceType: Driver["evidence_type"] }) {
  const c = CONF[confidence] ?? CONF.low;
  return (
    <span className="inline-flex items-center gap-2">
      <span className="chip gap-2 py-0.5">
        <span className="flex gap-0.5" aria-hidden>
          {[0, 1, 2].map((i) => (
            <span key={i} className={`h-1.5 w-1.5 rounded-full ${i < c.dots ? "bg-ink" : "bg-line"}`} />
          ))}
        </span>
        <span className="text-ink/90">{c.label}</span>
      </span>
      <span className="text-xs text-muted">{EVIDENCE[evidenceType] ?? ""}</span>
    </span>
  );
}

export function Eyebrow({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={`eyebrow ${className}`}>{children}</div>;
}
