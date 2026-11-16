// Routes for the four screens and the keys: 1-4 screens, G gateway drawer, H how it works, R reset, F recorded copy.
import { Component, useEffect, type ErrorInfo, type ReactNode } from "react";
import { Navigate, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import { AnimatePresence, motion } from "framer-motion";
import { useQueryClient } from "@tanstack/react-query";
import { NAV, Shell, useShell } from "@/components/Shell";
import { ProvenanceProvider } from "@/components/Provenance";
import { ErrorState } from "@/components/ui";
import { api, friendlyMessage, recordedMode, RESET_EVENT } from "@/lib/api";
import { OneTruth } from "@/screens/OneTruth";
import { Forecast } from "@/screens/Forecast";
import { Ask } from "@/screens/Ask";
import { SaveSubscribers } from "@/screens/Save";

export function App() {
  return (
    <ProvenanceProvider>
      <Shell>
        <Hotkeys />
        <Screens />
      </Shell>
    </ProvenanceProvider>
  );
}

function Screens() {
  const location = useLocation();
  return (
    <AnimatePresence mode="wait">
      <motion.div key={location.pathname} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -4 }}
        transition={{ duration: 0.22, ease: "easeOut" }}>
        <Boundary key={location.pathname}>
          <Routes location={location}>
            <Route path="/" element={<OneTruth />} />
            <Route path="/forecast" element={<Forecast />} />
            <Route path="/ask" element={<Ask />} />
            <Route path="/save" element={<SaveSubscribers />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Boundary>
      </motion.div>
    </AnimatePresence>
  );
}

function Hotkeys() {
  const qc = useQueryClient();
  const navigate = useNavigate();
  const { toggleDrawer, toggleGateway, notify } = useShell();
  useEffect(() => {
    const onKey = async (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const el = e.target as HTMLElement | null;
      if (el && (el.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(el.tagName))) return;
      const k = e.key.toLowerCase();
      const screen = /^[1-4]$/.test(k) ? NAV[Number(k) - 1] : undefined;
      if (screen) navigate(screen.to);
      else if (k === "h") toggleDrawer();
      else if (k === "g") toggleGateway();
      else if (k === "r") {
        try {
          await api.reset();
          window.dispatchEvent(new Event(RESET_EVENT));
          await qc.invalidateQueries();
          notify("Reset");
        } catch (err) {
          notify(friendlyMessage(err));
        }
      } else if (k === "f") {
        const next = !recordedMode.get();
        recordedMode.set(next);
        await qc.invalidateQueries({ queryKey: ["health"] });
        notify(next ? "Recorded copy on" : "Live calls on");
      } else return;
      e.preventDefault();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [qc, navigate, toggleDrawer, toggleGateway, notify]);
  return null;
}

class Boundary extends Component<{ children: ReactNode }, { error: unknown }> {
  state = { error: null as unknown };
  static getDerivedStateFromError(error: unknown) {
    return { error };
  }
  componentDidCatch(error: unknown, info: ErrorInfo) {
    console.warn("Screen error", error, info.componentStack);
  }
  render() {
    if (this.state.error) {
      return <ErrorState title="Something on this screen didn't load" error={this.state.error} onRetry={() => this.setState({ error: null })} />;
    }
    return this.props.children;
  }
}
