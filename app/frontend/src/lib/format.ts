// Formatting only. Business numbers come from the payload; nothing here derives them.
const ET = "America/New_York";
const clockFmt = new Intl.DateTimeFormat("en-US", { timeZone: ET, hour: "numeric", minute: "2-digit" });

/** "8:17 PM ET". */
export function clockLabel(v: string | number, withZone = true): string {
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "";
  return withZone ? `${clockFmt.format(d)} ET` : clockFmt.format(d);
}

/** "8:15" or "6 PM" for axes. */
export function axisClock(ms: number): string {
  const parts = clockFmt.formatToParts(new Date(ms));
  const h = parts.find((p) => p.type === "hour")?.value ?? "";
  const m = parts.find((p) => p.type === "minute")?.value ?? "";
  const pm = parts.find((p) => p.type === "dayPeriod")?.value ?? "";
  return m === "00" ? `${h} ${pm}` : `${h}:${m}`;
}

/** 9,500,000 -> "9.5M", 15,800 -> "15.8k". */
export function compact(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e6) return `${trim((n / 1e6).toFixed(1))}M`;
  if (abs >= 1e5) return `${Math.round(n / 1e3)}k`;
  if (abs >= 1e3) return `${trim((n / 1e3).toFixed(1))}k`;
  return `${Math.round(n)}`;
}

/** 4900 -> "4,900". */
export function count(n: number): string {
  return Math.round(n).toLocaleString("en-US");
}

/** 1702 -> "~1,700". */
export function approx(n: number): string {
  return `~${count(n >= 1000 ? Math.round(n / 100) * 100 : n)}`;
}

/** 245000 -> "$245k", 2000 -> "$2.0k", 450 -> "$450". */
export function usdShort(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e6) return `$${trim((n / 1e6).toFixed(2))}M`;
  if (abs >= 1e5) return `$${Math.round(n / 1e3)}k`;
  if (abs >= 1e3) return `$${(n / 1e3).toFixed(1)}k`;
  return `$${Math.round(n).toLocaleString("en-US")}`;
}

export function usdExact(n: number): string {
  return `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/** 0.34 -> "34%", 0.0025 -> "0.25%". */
export function pct(ratio: number, digits?: number): string {
  const v = ratio * 100;
  const d = digits ?? (Math.abs(v) < 1 && v !== 0 ? 2 : 0);
  return `${trim(v.toFixed(d))}%`;
}

export function durationS(ms: number): string {
  return `${(ms / 1000).toFixed(1)}s`;
}

function trim(s: string): string {
  return s.includes(".") ? s.replace(/\.0+$/, "").replace(/(\.\d*?)0+$/, "$1") : s;
}
