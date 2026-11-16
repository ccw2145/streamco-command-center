// Forecast: a replay (play/scrub) of forecast vs actual viewers, the "flagged before kickoff" callout and the alert.
// The replay clock runs in the browser; it pauses once when the anomaly rule flags the cell.
import { useEffect, useMemo, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { Area, ComposedChart, Line, ReferenceArea, ReferenceDot, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { useScreen } from "@/lib/useScreen";
import { HowItWorksDrawer } from "@/components/Drawer";
import { ProvenanceNumber } from "@/components/Provenance";
import { Check, ErrorState, ScreenSkeleton } from "@/components/ui";
import { RESET_EVENT } from "@/lib/api";
import { axisClock, clockLabel, compact, count, pct } from "@/lib/format";
import type { Clock, ForecastPayload, SeriesPoint } from "@/lib/types";

const SPEEDS = [30, 60, 120, 240];

export function Forecast() {
  const q = useScreen("forecast");
  if (q.isPending) return <ScreenSkeleton tiles={2} />;
  if (q.isError) return <ErrorState error={q.error} onRetry={() => q.refetch()} />;
  return <ForecastView d={q.data} />;
}

interface Replay {
  t: number;
  playing: boolean;
  speed: number;
  toggle: () => void;
  seek: (t: number) => void;
  setSpeed: (s: number) => void;
}

function useReplay(c: Clock, pauseAt: number): Replay {
  const t0 = Date.parse(c.t0);
  const tEnd = Date.parse(c.end);
  const [t, setTState] = useState(t0);
  const tRef = useRef(t0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(120);
  const paused = useRef(false);   // the one automatic pause has happened
  const setT = (v: number) => {
    tRef.current = v;
    setTState(v);
  };

  useEffect(() => {
    if (!playing) return;
    let last = performance.now();
    const id = setInterval(() => {
      const now = performance.now();
      const prev = tRef.current;
      let next = Math.min(prev + (now - last) * speed, tEnd);
      last = now;
      if (!paused.current && prev < pauseAt && next >= pauseAt) {
        paused.current = true;
        next = pauseAt;
        setPlaying(false);
      }
      if (next >= tEnd) setPlaying(false);
      tRef.current = next;
      setTState(next);
    }, 100);
    return () => clearInterval(id);
  }, [playing, speed, tEnd, pauseAt]);

  useEffect(() => {
    const reset = () => {
      setPlaying(false);
      tRef.current = t0;
      setTState(t0);
      paused.current = false;
    };
    window.addEventListener(RESET_EVENT, reset);
    return () => window.removeEventListener(RESET_EVENT, reset);
  }, [t0]);

  return {
    t,
    playing,
    speed,
    toggle: () => {
      if (!playing && tRef.current >= tEnd) setT(t0);
      setPlaying((p) => !p);
    },
    seek: (v) => setT(Math.min(Math.max(v, t0), tEnd)),
    setSpeed,
  };
}

function ForecastView({ d }: { d: ForecastPayload }) {
  const a = d.alert;
  const replay = useReplay(d.clock, Date.parse(a.detected_t));
  const t = replay.t;
  const reached = t >= Date.parse(a.onset_t);
  const watching = useMemo(() => interpolate(d.series, t), [d.series, t]);

  return (
    <div>
      <header className="flex items-end justify-between gap-8">
        <div>
          <div className="eyebrow">2 of 4 &middot; Replay of the night &middot; Could we have seen it coming?</div>
          <h1 className="mt-2 text-[2.2rem] font-semibold tracking-tight">The forecast, before kickoff, against what happened</h1>
        </div>
        <ReplayControls r={replay} c={d.clock} />
      </header>

      <div className="mt-6 grid grid-cols-12 gap-5">
        <section className="card col-span-8 p-5">
          <div className="flex items-baseline justify-between">
            <div className="flex items-baseline gap-4">
              <h2 className="text-[1.05rem] font-semibold">Viewers vs forecast</h2>
              <span className="tnum text-sm text-muted">{clockLabel(t)}</span>
            </div>
            <Legend />
          </div>
          <ForecastChart series={d.series} clock={d.clock} cursor={t} />
        </section>

        <div className="col-span-4 space-y-5">
          <section className="card grid grid-cols-[1.25fr_1fr_1fr] gap-4 px-6 py-5">
            <div>
              <div className="eyebrow">{phase(d.clock, t)}</div>
              <div className="tnum mt-1.5 text-num-sm">{clockLabel(t, false)}</div>
            </div>
            <div>
              <div className="text-xs text-muted">Watching now</div>
              <div className="tnum mt-1.5 text-num-sm">{watching !== null ? compact(watching) : "—"}</div>
            </div>
            <div>
              <div className="text-xs text-muted">Peak</div>
              <div className="tnum mt-1.5 text-num-sm">
                <ProvenanceNumber metricId="peak_viewers">{compact(d.peak.actual)}</ProvenanceNumber>
              </div>
              <div className="tnum mt-1 text-[0.7rem] text-muted">forecast {compact(d.peak.forecast)}</div>
            </div>
          </section>

          <section className="card relative overflow-hidden p-5" aria-label="Flagged before kickoff">
            <span className="absolute inset-y-0 left-0 w-1 bg-action" aria-hidden />
            <div className="eyebrow">Forecast &middot; before kickoff</div>
            <p className="mt-2 text-[1.08rem] font-semibold leading-snug">
              Flagged {d.callout.cdn} {d.callout.metro_name.split("-")[0]} at{" "}
              <ProvenanceNumber metricId="hot_cell_utilization" className="text-action">{pct(d.callout.util)}</ProvenanceNumber> of capacity
            </p>
            <p className="mt-1.5 text-sm text-muted">{d.callout.detail}</p>
          </section>

          <AnimatePresence mode="wait" initial={false}>
            {reached ? (
              <motion.div key="alert" initial={{ opacity: 0, y: 14, scale: 0.98 }} animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0 }} transition={{ type: "spring", stiffness: 300, damping: 26 }}>
                <AlertCard a={a} t={t} />
              </motion.div>
            ) : (
              <motion.section key="calm" className="card p-6" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                <div className="flex items-center gap-2.5">
                  <span className="grid h-6 w-6 place-items-center rounded-full bg-ok/15 text-ok"><Check className="h-3.5 w-3.5" /></span>
                  <span className="font-semibold">All streams healthy</span>
                </div>
                <p className="mt-3 text-sm leading-relaxed text-muted">
                  The anomaly rule watches every network and city, minute by minute, against its normal range.
                </p>
                <button className="mt-4 text-sm text-muted transition hover:text-ink"
                  onClick={() => replay.seek(Date.parse(a.onset_t) - 4 * 60_000)}>
                  Skip to halftime &rarr;
                </button>
              </motion.section>
            )}
          </AnimatePresence>
        </div>
      </div>

      <HowItWorksDrawer hiw={d.how_it_works} />
    </div>
  );
}

function AlertCard({ a, t }: { a: ForecastPayload["alert"]; t: number }) {
  const shifted = t >= Date.parse(a.ops.t);
  const resolved = t >= Date.parse(a.ops.resolved_t);
  const detected = t >= Date.parse(a.detected_t);
  return (
    <section className={`card relative overflow-hidden p-6 ${resolved ? "" : "border-alert/50"}`} role="alert">
      {!resolved && <span className="absolute inset-x-0 top-0 h-1 bg-alert" aria-hidden />}
      <div className="flex items-center justify-between">
        <span className={`eyebrow ${resolved ? "" : "!text-alert"}`}>{resolved ? "Resolved" : "Alert"} &middot; {clockLabel(a.onset_t)}</span>
        <span className="tnum text-sm text-muted">
          <ProvenanceNumber metricId="affected_sessions">{compact(a.affected_sessions)}</ProvenanceNumber> sessions
        </span>
      </div>
      <h2 className="mt-2.5 text-[1.35rem] font-semibold leading-tight tracking-tight">{a.headline}</h2>
      <div className="mt-3 flex items-center gap-4 rounded-xl border border-alert/30 bg-alert/[0.06] px-4 py-2.5">
        <span className="tnum text-[1.6rem] font-semibold text-alert">{pct(a.observed, 1)}</span>
        <span className="text-xs leading-snug text-muted">
          <span className="text-ink">{a.cohort_label}</span>
          <br />
          normal {pct(a.baseline, 1)}
        </span>
      </div>
      <div className="mt-4 flex items-center gap-3">
        <span className={`tnum rounded-full border px-3 py-1 text-sm font-medium ${detected ? "border-ok/40 bg-ok/10 text-ok" : "border-line text-muted"}`}>
          {detected ? `Flagged within ${a.minutes_to_detect} min` : "Checking the next minutes..."}
        </span>
        {detected && <span className="tnum text-sm text-muted">at {clockLabel(a.detected_t)}</span>}
      </div>
      <p className="mt-3 text-[0.9rem] leading-relaxed text-muted">{a.explanation}</p>
      {shifted && (
        <motion.p initial={{ opacity: 0 }} animate={{ opacity: 1 }}
          className={`mt-4 flex items-center gap-2 border-t border-line pt-4 text-[0.98rem] font-medium ${resolved ? "text-ok" : "text-ink"}`}>
          {resolved && <Check className="h-4 w-4 shrink-0" />}
          {a.ops.label}
        </motion.p>
      )}
    </section>
  );
}

function interpolate(series: SeriesPoint[], t: number): number | null {
  if (!series.length) return null;
  const pts = series.map((s) => ({ t: Date.parse(s.t), v: s.actual }));
  if (t <= pts[0].t) return pts[0].v;
  for (let i = 0; i < pts.length - 1; i++) {
    if (t >= pts[i].t && t <= pts[i + 1].t) return pts[i].v + ((t - pts[i].t) / (pts[i + 1].t - pts[i].t)) * (pts[i + 1].v - pts[i].v);
  }
  return pts[pts.length - 1].v;
}

function phase(c: Clock, t: number): string {
  if (t < Date.parse(c.kickoff)) return "Pre-game";
  if (t < Date.parse(c.halftime_start)) return "First half";
  if (t < Date.parse(c.halftime_end)) return "Halftime show";
  if (t < Date.parse(c.end)) return "Second half";
  return "Final";
}

function Legend() {
  return (
    <div className="flex items-center gap-4 text-xs text-muted">
      <span className="inline-flex items-center gap-1.5"><span className="h-[3px] w-4 rounded bg-ink" /> Actual</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-4 border-t border-dashed border-action" /> Forecast</span>
      <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-4 rounded-sm bg-action/20" /> 80% range</span>
    </div>
  );
}

function ReplayControls({ r, c }: { r: Replay; c: Clock }) {
  const t0 = Date.parse(c.t0);
  const tEnd = Date.parse(c.end);
  const span = tEnd - t0 || 1;
  const marks = [{ label: "Kickoff", t: Date.parse(c.kickoff) }, { label: "Halftime", t: Date.parse(c.halftime_start) }];
  return (
    <div className="flex items-center gap-5" aria-label="Replay controls">
      <button onClick={r.toggle} className="grid h-11 w-11 place-items-center rounded-full bg-ink text-canvas transition hover:bg-white"
        aria-label={r.playing ? "Pause" : "Play"} title={r.playing ? "Pause" : "Play"}>
        {r.playing ? (
          <svg viewBox="0 0 20 20" className="h-4 w-4" fill="currentColor" aria-hidden>
            <rect x="4.5" y="3.5" width="3.8" height="13" rx="1" /><rect x="11.7" y="3.5" width="3.8" height="13" rx="1" />
          </svg>
        ) : (
          <svg viewBox="0 0 20 20" className="ml-0.5 h-4 w-4" fill="currentColor" aria-hidden><path d="M5 3.5v13l11-6.5z" /></svg>
        )}
      </button>
      <div className="relative w-[340px] pt-5">
        {marks.map((m) => (
          <button key={m.label} className="absolute top-0 -translate-x-1/2 text-[0.68rem] text-faint transition hover:text-ink"
            style={{ left: `${((m.t - t0) / span) * 100}%` }} onClick={() => r.seek(m.t)} title={`Jump to ${m.label.toLowerCase()}`}>
            {m.label}
          </button>
        ))}
        <input type="range" min={t0} max={tEnd} step={60_000} value={r.t} onChange={(e) => r.seek(Number(e.target.value))}
          className="h-1.5 w-full cursor-pointer appearance-none rounded-full bg-raised accent-[#EEF1F7]" aria-label="Replay position" />
        <div className="mt-1 flex justify-between text-[0.68rem] text-faint">
          <span>{clockLabel(t0, false)}</span>
          <span>{clockLabel(tEnd, false)}</span>
        </div>
      </div>
      <div className="flex rounded-lg border border-line p-0.5" role="group" aria-label="Replay speed">
        {SPEEDS.map((s) => (
          <button key={s} onClick={() => r.setSpeed(s)} aria-pressed={s === r.speed}
            className={`tnum rounded-md px-2.5 py-1 text-xs transition ${s === r.speed ? "bg-raised text-ink" : "text-muted hover:text-ink"}`}>
            {s}&times;
          </button>
        ))}
      </div>
    </div>
  );
}

interface Row {
  t: number;
  band: [number, number];
  yhat: number;
  actual: number | null;
}

function ForecastChart({ series, clock, cursor }: { series: SeriesPoint[]; clock: Clock; cursor: number }) {
  const base = useMemo(() => series.map((p) => ({ ...p, ms: Date.parse(p.t) })).sort((a, b) => a.ms - b.ms), [series]);
  // Reveal the actual line up to the cursor, with an interpolated point at the cursor itself.
  const { rows, now } = useMemo(() => {
    const out: Row[] = [];
    let nowPt: { t: number; v: number } | null = null;
    base.forEach((p, i) => {
      if (p.ms <= cursor) {
        out.push({ t: p.ms, band: [p.lo, p.hi], yhat: p.yhat, actual: p.actual });
        nowPt = { t: p.ms, v: p.actual };
        const next = base[i + 1];
        if (next && next.ms > cursor) {
          const f = (cursor - p.ms) / (next.ms - p.ms);
          const lerp = (a: number, b: number) => a + f * (b - a);
          const v = lerp(p.actual, next.actual);
          out.push({ t: cursor, band: [lerp(p.lo, next.lo), lerp(p.hi, next.hi)], yhat: lerp(p.yhat, next.yhat), actual: v });
          nowPt = { t: cursor, v };
        }
      } else {
        out.push({ t: p.ms, band: [p.lo, p.hi], yhat: p.yhat, actual: null });
      }
    });
    return { rows: out, now: nowPt as { t: number; v: number } | null };
  }, [base, cursor]);

  if (!base.length) return <div className="grid h-[520px] place-items-center text-sm text-muted">No forecast yet.</div>;
  const domain: [number, number] = [base[0].ms, base[base.length - 1].ms];
  const yMaxRaw = Math.max(...base.map((b) => Math.max(b.hi, b.actual)));
  const yStep = yMaxRaw > 6e6 ? 2.5e6 : yMaxRaw > 2e6 ? 1e6 : Math.max(1, 10 ** Math.floor(Math.log10(yMaxRaw || 1)));
  const yMax = Math.ceil((yMaxRaw * 1.05) / yStep) * yStep;
  const yTicks = Array.from({ length: Math.round(yMax / yStep) + 1 }, (_, i) => i * yStep);
  const ticks = [clock.t0, clock.kickoff, clock.halftime_start, clock.halftime_end, clock.end].map((s) => Date.parse(s));

  return (
    <div style={{ height: 520 }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={rows} margin={{ top: 16, right: 16, bottom: 4, left: 4 }}>
          <defs>
            <linearGradient id="bandFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#8FB3FF" stopOpacity={0.22} />
              <stop offset="100%" stopColor="#8FB3FF" stopOpacity={0.1} />
            </linearGradient>
          </defs>
          <ReferenceArea x1={Date.parse(clock.halftime_start)} x2={Date.parse(clock.halftime_end)} fill="#ffffff" fillOpacity={0.035}
            label={{ value: "Halftime", position: "insideTop", fill: "#9AA3B5", fontSize: 12 }} />
          <XAxis dataKey="t" type="number" domain={domain} ticks={ticks} tickFormatter={(v: number) => axisClock(v)}
            stroke="#232B3D" tick={{ fill: "#9AA3B5", fontSize: 12 }} tickLine={false} />
          <YAxis tickFormatter={(v: number) => compact(v)} stroke="#232B3D" tick={{ fill: "#9AA3B5", fontSize: 12 }}
            tickLine={false} axisLine={false} width={44} domain={[0, yMax]} ticks={yTicks} />
          <Tooltip cursor={{ stroke: "#4A5670" }}
            contentStyle={{ background: "#171D2B", border: "1px solid #232B3D", borderRadius: 10, fontSize: 12 }}
            labelFormatter={(v: number) => clockLabel(v)}
            formatter={(value: unknown, name: string) =>
              Array.isArray(value)
                ? [`${count(value[0] as number)} to ${count(value[1] as number)}`, "Forecast range (80%)"]
                : [compact(value as number), name === "yhat" ? "Forecast" : "Actual"]} />
          <Area dataKey="band" stroke="none" fill="url(#bandFill)" isAnimationActive={false} activeDot={false} />
          <Line dataKey="yhat" stroke="#8FB3FF" strokeOpacity={0.7} strokeDasharray="4 4" dot={false} strokeWidth={1.5} isAnimationActive={false} />
          <Line dataKey="actual" stroke="#EEF1F7" strokeWidth={2.6} dot={false} connectNulls={false} isAnimationActive={false} />
          <ReferenceLine x={cursor} stroke="#EEF1F7" strokeOpacity={0.35} />
          {now && <ReferenceDot x={now.t} y={now.v} r={5} fill="#EEF1F7" stroke="#0A0D14" strokeWidth={2} />}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
