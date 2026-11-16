// Ask: question -> lanes (tool steps, incl. AI Search) -> answer card (34/29/30/7 with SQL, document citations) -> Genie follow-ups.
import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { useScreen } from "@/lib/useScreen";
import { HowItWorksDrawer, Section } from "@/components/Drawer";
import { SqlBlock } from "@/components/Provenance";
import { ErrorState, ScreenSkeleton } from "@/components/ui";
import { api, friendlyMessage, RESET_EVENT } from "@/lib/api";
import { durationS } from "@/lib/format";
import type { AskEvent, AskPayload } from "@/lib/types";
import { AnswerCard, GenieCard, Lanes, PendingAnswer, type CitedAnswer, type FollowupItem, type LaneState } from "./AskParts";

export function Ask() {
  const q = useScreen("ask");
  if (q.isPending) return <ScreenSkeleton tiles={1} />;
  if (q.isError) return <ErrorState error={q.error} onRetry={() => q.refetch()} />;
  return <AskView d={q.data} />;
}

interface Run {
  question: string;
  startedAt: number;
  answeredAt: number | null;
  lanes: LaneState[];
  answer: CitedAnswer | null;
  error: string | null;
  recorded: boolean;
}

function reduce(r: Run, e: AskEvent, now: number): Run {
  switch (e.event) {
    case "lane_start":
      // A lane the screen payload doesn't list yet (e.g. a newer backend) is added rather than dropped.
      if (!r.lanes.some((l) => l.def.lane === e.data.lane)) {
        return { ...r, lanes: [...r.lanes, { def: e.data, status: "running", startedAt: now }] };
      }
      return { ...r, lanes: r.lanes.map((l) => (l.def.lane === e.data.lane ? { ...l, status: "running", startedAt: now } : l)) };
    case "lane_done":
      return {
        ...r,
        lanes: r.lanes.map((l) =>
          l.def.lane === e.data.lane ? { ...l, status: "done", done: e.data, startedAt: l.startedAt ?? now, finishedAt: now } : l),
      };
    case "answer":
      return {
        ...r,
        answer: e.data,
        answeredAt: now,
        lanes: r.lanes.map((l) => (l.status === "running" ? { ...l, status: "done", finishedAt: now } : l)),
      };
    case "fallback":
      // Start the lanes over for the recorded replay that follows.
      return { ...r, lanes: r.lanes.map((l) => ({ def: l.def, status: "waiting" })), answer: null, answeredAt: null, startedAt: now, recorded: true };
    case "done":
      return { ...r, recorded: r.recorded || e.data.recorded };
    case "error":
      return { ...r, error: e.data.message };
    default:
      return r;
  }
}

function AskView({ d }: { d: AskPayload }) {
  const [input, setInput] = useState("");
  const [typing, setTyping] = useState(false);
  const [run, setRun] = useState<Run | null>(null);
  const [followups, setFollowups] = useState<FollowupItem[]>([]);
  const [fuInput, setFuInput] = useState("");
  const conversationId = useRef<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const seq = useRef(0);
  const typeTimer = useRef<ReturnType<typeof setInterval>>();
  const latestRef = useRef<HTMLDivElement>(null);
  const nFollowups = followups.length;
  useEffect(() => {
    if (nFollowups) latestRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [nFollowups]);

  const clearAll = useCallback(() => {
    abortRef.current?.abort();
    if (typeTimer.current) clearInterval(typeTimer.current);
    setTyping(false);
    setRun(null);
    setFollowups([]);
    setInput("");
    conversationId.current = null;
  }, []);

  useEffect(() => {
    window.addEventListener(RESET_EVENT, clearAll);
    return () => {
      window.removeEventListener(RESET_EVENT, clearAll);
      abortRef.current?.abort();
      if (typeTimer.current) clearInterval(typeTimer.current);
    };
  }, [clearAll]);

  const start = useCallback((question: string) => {
    const qtext = question.trim();
    if (!qtext) return;
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setFollowups([]);
    conversationId.current = null;
    setRun({ question: qtext, startedAt: performance.now(), answeredAt: null, answer: null, error: null, recorded: false,
      lanes: d.lanes.map((def) => ({ def, status: "waiting" })) });
    api
      .ask(qtext, (e) => !ctrl.signal.aborted && setRun((r) => (r ? reduce(r, e, performance.now()) : r)), ctrl.signal)
      .then(() => {
        if (ctrl.signal.aborted) return;
        setRun((r) => (r && !r.answer && !r.error ? { ...r, error: "The investigation ended without an answer. Try asking again." } : r));
      })
      .catch((err: unknown) => {
        if (!ctrl.signal.aborted) setRun((r) => (r ? { ...r, error: friendlyMessage(err) } : r));
      });
  }, [d.lanes]);

  /** Clicking the suggestion "types" it, so the room sees a question being asked. */
  const askTyped = (question: string) => {
    if (typeTimer.current) clearInterval(typeTimer.current);
    setTyping(true);
    let i = 0;
    typeTimer.current = setInterval(() => {
      i += 2;
      setInput(question.slice(0, i));
      if (i >= question.length) {
        clearInterval(typeTimer.current);
        setTyping(false);
        start(question);
      }
    }, 22);
  };

  const askFollowup = (question: string) => {
    const qtext = question.trim();
    if (!qtext) return;
    const id = ++seq.current;
    setFollowups((f) => [...f, { id, question: qtext, startedAt: performance.now() }]);
    setFuInput("");
    api
      .followup(qtext, conversationId.current)
      .then((answer) => {
        if (answer.conversation_id && !answer.recorded) conversationId.current = answer.conversation_id;
        setFollowups((f) => f.map((x) => (x.id === id ? { ...x, answer } : x)));
      })
      .catch((error: unknown) => setFollowups((f) => f.map((x) => (x.id === id ? { ...x, error } : x))));
  };

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!typing) start(input);
  };

  const running = run !== null && run.answer === null && run.error === null;
  const answered = run?.answer && !run.answer.declined;
  const asked = new Set(followups.map((f) => f.question));
  const allDone = Boolean(run?.lanes.every((l) => l.status === "done"));
  const declinedOnly = run?.answer && run.lanes.every((l) => l.status === "waiting");

  return (
    <div>
      <header className={run ? "" : "mx-auto max-w-4xl pt-[8vh] text-center"}>
        {!run && (
          <>
            <div className="eyebrow">3 of 4 &middot; Why did they cancel? &middot; Ask in plain English</div>
            <h1 className="mt-3 text-[2.6rem] font-semibold tracking-tight">What do you want to know about last night?</h1>
          </>
        )}
        <form onSubmit={onSubmit} className={`${run ? "" : "mt-8"} flex items-center gap-3`}>
          <input value={input} onChange={(e) => setInput(e.target.value)} placeholder={d.primary_question} aria-label="Your question"
            readOnly={typing}
            className={`w-full flex-1 rounded-2xl border border-line bg-surface px-6 text-ink placeholder:text-faint focus:border-faint focus:outline-none ${
              run ? "py-4 text-[1.1rem]" : "py-5 text-[1.3rem]"}`} />
          <button type="submit" className="btn-primary h-[3.6rem] px-7" disabled={typing || running || !input.trim()}>
            {running ? "Investigating..." : "Ask"}
          </button>
        </form>
        {!run && (
          <div className="mt-5 flex justify-center">
            <button onClick={() => askTyped(d.primary_question)} disabled={typing}
              className="rounded-full border border-faint bg-raised px-4 py-2 text-[0.95rem] text-ink transition hover:border-ink/60">
              {d.primary_question}
            </button>
          </div>
        )}
      </header>

      {run && declinedOnly && run.answer && (
        <div className="mx-auto mt-8 max-w-3xl">
          <AnswerCard answer={run.answer} recorded={run.recorded} onAsk={askTyped} primary={d.primary_question} />
        </div>
      )}

      {run && !declinedOnly && (
        <div className="mt-7 grid grid-cols-12 gap-6">
          <div className="col-span-5">
            <Lanes lanes={run.lanes} startedAt={run.startedAt} finishedAt={run.answeredAt ?? (run.error ? performance.now() : null)} />
            {answered && (
              <motion.section className="mt-8" initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.6 }}
                aria-label="Follow-up questions">
                <h2 className="text-[1.15rem] font-semibold">Ask a follow-up</h2>
                <p className="mt-1 text-sm text-muted">Answered directly by the Genie Agent, with its SQL one click away.</p>
                <div className="mt-4 flex flex-wrap items-center gap-2">
                  {d.followup_suggestions.filter((s) => !asked.has(s)).map((s) => (
                    <button key={s} onClick={() => askFollowup(s)}
                      className="rounded-full border border-line px-4 py-2 text-[0.95rem] text-ink transition hover:border-faint">
                      {s}
                    </button>
                  ))}
                  <form className="flex w-full items-center gap-2" onSubmit={(e) => { e.preventDefault(); askFollowup(fuInput); }}>
                    <input value={fuInput} onChange={(e) => setFuInput(e.target.value)} placeholder="Or type your own follow-up..."
                      aria-label="Follow-up question"
                      className="w-full rounded-xl border border-line bg-surface px-4 py-2.5 text-[0.95rem] placeholder:text-faint focus:border-faint focus:outline-none" />
                    <button type="submit" className="btn-ghost text-ink" disabled={!fuInput.trim()}>Ask</button>
                  </form>
                </div>
                <div className="mt-5 space-y-4">
                  {[...followups].reverse().map((f, i) => (
                    <div key={f.id} ref={i === 0 ? latestRef : undefined} className="scroll-mt-24">
                      <GenieCard item={f} onRetry={() => { setFollowups((x) => x.filter((y) => y.id !== f.id)); askFollowup(f.question); }} />
                    </div>
                  ))}
                </div>
              </motion.section>
            )}
          </div>
          <div className="col-span-7">
            <AnimatePresence mode="wait">
              {run.answer ? (
                <motion.div key="answer" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
                  <AnswerCard answer={run.answer} recorded={run.recorded} onAsk={askTyped} primary={d.primary_question} />
                  {run.answeredAt && !run.answer.declined && (
                    <p className="tnum mt-2 text-right text-xs text-faint">Answered in {durationS(run.answeredAt - run.startedAt)}</p>
                  )}
                </motion.div>
              ) : run.error ? (
                <motion.div key="err" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
                  <ErrorState title="The investigation didn't finish" error={new Error(run.error)} onRetry={() => start(run.question)} />
                </motion.div>
              ) : (
                <motion.div key="pending" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                  <PendingAnswer allDone={allDone} />
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </div>
      )}

      <HowItWorksDrawer hiw={d.how_it_works}>
        {run && run.lanes.some((l) => l.done?.sql_text) && (
          <Section title="The SQL behind each lane">
            <div className="space-y-4">
              {run.lanes.filter((l) => l.done?.sql_text).map((l) => (
                <div key={l.def.lane}>
                  <div className="mb-1.5 text-sm font-medium">{l.def.label}</div>
                  <SqlBlock sql={l.done!.sql_text!} className="text-[0.72rem]" />
                </div>
              ))}
            </div>
          </Section>
        )}
      </HowItWorksDrawer>
    </div>
  );
}
