// Typed fetch wrapper, the Ask SSE reader and the recorded-mode switch (F key, sent as X-Mode: recorded).
import { useSyncExternalStore } from "react";
import type { AskEvent, Approval, GenieAnswer, Health, Provenance, RegenerateResponse, ScreenId, ScreenMap, Serving } from "./types";

export const RESET_EVENT = "sc:reset";
const OFFLINE = "We couldn't reach the app just now. Try again, or press F for the recorded copy.";

export class ApiError extends Error {}

// ---------------- recorded mode (per browser tab)
type Listener = () => void;
const listeners = new Set<Listener>();
let forceRecorded = sessionStorage.getItem("sc:recorded") === "1";

export const recordedMode = {
  get: () => forceRecorded,
  set(v: boolean) {
    forceRecorded = v;
    sessionStorage.setItem("sc:recorded", v ? "1" : "0");
    listeners.forEach((l) => l());
  },
  subscribe(l: Listener) {
    listeners.add(l);
    return () => listeners.delete(l);
  },
};

export function useRecordedMode(): boolean {
  return useSyncExternalStore(recordedMode.subscribe, recordedMode.get, recordedMode.get);
}

function headers(json: boolean): Record<string, string> {
  const h: Record<string, string> = { Accept: json ? "application/json" : "text/event-stream" };
  if (json) h["Content-Type"] = "application/json";
  if (forceRecorded) h["X-Mode"] = "recorded";
  return h;
}

async function request<T>(method: "GET" | "POST", path: string, body?: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path, { method, headers: headers(true), body: body === undefined ? undefined : JSON.stringify(body) });
  } catch {
    throw new ApiError(OFFLINE);
  }
  if (!res.ok) {
    let msg = res.status === 404 ? "That item isn't available in this demo." : OFFLINE;
    try {
      const j = (await res.json()) as { error?: { message?: string } };
      if (j.error?.message) msg = j.error.message;
    } catch {
      /* not JSON */
    }
    throw new ApiError(msg);
  }
  return (await res.json()) as T;
}

/** POST /api/ask and read its server-sent events until `done`. */
async function ask(question: string, onEvent: (e: AskEvent) => void, signal: AbortSignal): Promise<void> {
  let res: Response;
  try {
    res = await fetch("/api/ask", { method: "POST", headers: { ...headers(true), Accept: "text/event-stream" },
      body: JSON.stringify({ question }), signal });
  } catch (e) {
    if (signal.aborted) return;
    throw new ApiError(OFFLINE);
  }
  if (!res.ok || !res.body) throw new ApiError("The investigation service didn't respond.");
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx: number;
    while ((idx = buf.indexOf("\n\n")) >= 0) {
      const frame = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      let event = "";
      let data = "";
      for (const line of frame.split("\n")) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) data += line.slice(5).trim();
      }
      if (event && data) onEvent({ event, data: JSON.parse(data) } as AskEvent);
    }
  }
}

export const api = {
  screen: <S extends ScreenId>(s: S) => request<ScreenMap[S]>("GET", `/api/screens/${s}`),
  provenance: (id: string) => request<Provenance>("GET", `/api/provenance/${encodeURIComponent(id)}`),
  health: () => request<Health>("GET", "/api/health"),
  ask,
  followup: (question: string, conversationId: string | null) =>
    request<GenieAnswer>("POST", "/api/ask/followup", { question, conversation_id: conversationId }),
  approve: () => request<Approval>("POST", "/api/save/approve"),
  regenerate: (sampleId: string) => request<RegenerateResponse>("POST", `/api/save/regenerate/${encodeURIComponent(sampleId)}`),
  serving: (sampleId: string) => request<Serving>("GET", `/api/save/serving/${encodeURIComponent(sampleId)}`),
  reset: () => request<{ ok: boolean }>("POST", "/api/reset"),
};

export function friendlyMessage(e: unknown): string {
  return e instanceof ApiError ? e.message : OFFLINE;
}
