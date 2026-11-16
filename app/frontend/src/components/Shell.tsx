// Header nav, synthetic-data banner, the two drawers' state, the mode corner and toasts.
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { AnimatePresence, motion } from "framer-motion";
import { useQuery } from "@tanstack/react-query";
import { api, useRecordedMode } from "@/lib/api";
import { GatewayDrawer } from "./GatewayDrawer";

interface ShellCtx {
  drawerOpen: boolean;
  setDrawerOpen: (v: boolean) => void;
  toggleDrawer: () => void;
  gatewayOpen: boolean;
  setGatewayOpen: (v: boolean) => void;
  toggleGateway: () => void;
  notify: (msg: string) => void;
}

const Ctx = createContext<ShellCtx | null>(null);

export function useShell(): ShellCtx {
  const c = useContext(Ctx);
  if (!c) throw new Error("useShell outside Shell");
  return c;
}

/** The four screens in demo order; keys 1-4 jump to them. */
export const NAV = [
  { to: "/", label: "One Truth" },
  { to: "/forecast", label: "Forecast" },
  { to: "/ask", label: "Ask" },
  { to: "/save", label: "Save Subscribers" },
];

export function Shell({ children }: { children: ReactNode }) {
  const [drawerOpen, setDrawer] = useState(false);
  const [gatewayOpen, setGateway] = useState(false);
  const [toast, setToast] = useState<{ id: number; msg: string } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout>>();
  const location = useLocation();

  const notify = useCallback((msg: string) => {
    setToast({ id: Date.now(), msg });
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setToast(null), 2600);
  }, []);
  // Opening one drawer closes the other.
  const setDrawerOpen = useCallback((v: boolean) => { setDrawer(v); if (v) setGateway(false); }, []);
  const setGatewayOpen = useCallback((v: boolean) => { setGateway(v); if (v) setDrawer(false); }, []);
  const toggleDrawer = useCallback(() => { setGateway(false); setDrawer((v) => !v); }, []);
  const toggleGateway = useCallback(() => { setDrawer(false); setGateway((v) => !v); }, []);

  useEffect(() => {
    setDrawer(false);
    setGateway(false);
  }, [location.pathname]);

  const value = useMemo(
    () => ({ drawerOpen, setDrawerOpen, toggleDrawer, gatewayOpen, setGatewayOpen, toggleGateway, notify }),
    [drawerOpen, setDrawerOpen, toggleDrawer, gatewayOpen, setGatewayOpen, toggleGateway, notify],
  );

  return (
    <Ctx.Provider value={value}>
      <div className="flex min-h-screen flex-col">
        <div className="flex h-7 items-center justify-center gap-2 border-b border-line/60 bg-surface text-[0.72rem] font-medium tracking-wide text-muted">
          <span className="h-1.5 w-1.5 rounded-full bg-muted" aria-hidden />
          SYNTHETIC DATA &middot; all figures, people and accounts are generated for this demo
        </div>
        <header className="sticky top-0 z-30 border-b border-line/70 bg-canvas/85 backdrop-blur-md">
          <div className="mx-auto flex h-16 max-w-[1680px] items-center gap-6 px-8">
            <NavLink to="/" className="flex items-center gap-3 text-[0.95rem] font-semibold tracking-tight">
              <Mark />
              <span>StreamCo Command Center</span>
            </NavLink>
            <nav className="flex flex-1 items-center gap-1" aria-label="Screens">
              {NAV.map((n, i) => (
                <NavLink
                  key={n.to}
                  to={n.to}
                  end={n.to === "/"}
                  className={({ isActive }) =>
                    `relative rounded-lg px-3.5 py-2 text-[0.92rem] transition ${isActive ? "text-ink" : "text-muted hover:text-ink"}`
                  }
                >
                  {({ isActive }) => (
                    <>
                      <span className="mr-1.5 text-faint tnum">{i + 1}</span>
                      {n.label}
                      {isActive && (
                        <motion.span layoutId="nav-underline" className="absolute inset-x-3 -bottom-[13px] h-[2px] rounded-full bg-ink" />
                      )}
                    </>
                  )}
                </NavLink>
              ))}
            </nav>
            <button onClick={toggleGateway} className={`btn-ghost ${gatewayOpen ? "border-ok/50 text-ink" : ""}`}
              aria-pressed={gatewayOpen} title="Governed by Unity Gateway (G)">
              <Shield />
              <span className="text-[0.9rem]">Governed by Unity Gateway</span>
              <kbd className="rounded border border-line px-1.5 text-[0.65rem] text-faint">G</kbd>
            </button>
            <button onClick={toggleDrawer} className={`btn-ghost ${drawerOpen ? "border-faint text-ink" : ""}`}
              aria-pressed={drawerOpen} title="How it works (H)">
              <span className="text-[0.9rem]">How it works</span>
              <kbd className="rounded border border-line px-1.5 text-[0.65rem] text-faint">H</kbd>
            </button>
          </div>
        </header>
        <main className="mx-auto w-full max-w-[1680px] flex-1 px-8 pb-16 pt-8">{children}</main>
        <GatewayDrawer open={gatewayOpen} onClose={() => setGateway(false)} />
        <ModeCorner />
        <AnimatePresence>
          {toast && (
            <motion.div key={toast.id} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 8 }}
              className="fixed bottom-6 left-1/2 z-[60] -translate-x-1/2 rounded-full border border-line bg-raised px-5 py-2.5 text-sm shadow-card"
              role="status">
              {toast.msg}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </Ctx.Provider>
  );
}

/** Discreet corner dot: green = live calls on, grey = recorded copy only. Hover for detail. */
function ModeCorner() {
  const forced = useRecordedMode();
  const health = useQuery({ queryKey: ["health", forced], queryFn: api.health, staleTime: 30_000 });
  const live = health.data?.live ?? false;
  const label = health.isError ? "Backend unreachable" : live ? "Live (falls back to recorded)" : "Recorded copy";
  return (
    <div className="group fixed bottom-3 left-3 z-20 flex items-center gap-2 rounded-full px-2 py-1 text-[0.68rem] text-faint opacity-60 transition hover:bg-raised hover:opacity-100"
      title="Data mode">
      <span className={`h-1.5 w-1.5 rounded-full ${live ? "bg-ok" : "bg-faint"}`} />
      <span className="hidden group-hover:inline">
        {label}
        {health.data?.illustrative ? " · illustrative numbers" : ""} · F toggles
      </span>
    </div>
  );
}

function Shield() {
  return (
    <svg viewBox="0 0 20 20" className="h-4 w-4 text-ok" fill="none" stroke="currentColor" strokeWidth={1.7} aria-hidden>
      <path d="M10 2.5 3.5 5v4.6c0 4 2.8 6.9 6.5 7.9 3.7-1 6.5-3.9 6.5-7.9V5L10 2.5Z" strokeLinejoin="round" />
      <path d="m7 10 2.1 2.1L13.2 8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function Mark() {
  return (
    <svg viewBox="0 0 28 28" className="h-7 w-7" aria-hidden>
      <rect width="28" height="28" rx="8" fill="#171D2B" stroke="#232B3D" />
      <circle cx="14" cy="14" r="6.5" fill="none" stroke="#EEF1F7" strokeWidth="2.4" />
      <circle cx="14" cy="14" r="1.8" fill="#EEF1F7" />
    </svg>
  );
}
