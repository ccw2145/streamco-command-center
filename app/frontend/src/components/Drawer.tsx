// Right-hand drawer frame, shared by "How it works" (per screen) and "Governed by Unity Gateway".
import { useEffect, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { AnimatePresence, motion } from "framer-motion";
import type { HowItWorks } from "@/lib/types";
import { useShell } from "./Shell";

export function DrawerFrame({ open, onClose, eyebrow, title, children }: {
  open: boolean;
  onClose: () => void;
  eyebrow: string;
  title: string;
  children: ReactNode;
}) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  return createPortal(
    <AnimatePresence>
      {open && (
        <>
          <motion.div className="fixed inset-0 z-40 bg-black/30" initial={{ opacity: 0 }} animate={{ opacity: 1 }}
            exit={{ opacity: 0 }} onClick={onClose} />
          <motion.aside
            className="scroll-thin fixed bottom-0 right-0 top-0 z-50 flex w-[min(660px,46vw)] min-w-[520px] flex-col overflow-y-auto border-l border-line bg-surface shadow-drawer"
            initial={{ x: "100%" }}
            animate={{ x: 0 }}
            exit={{ x: "100%" }}
            transition={{ type: "spring", stiffness: 380, damping: 40 }}
            aria-label={title}
            role="dialog"
          >
            <div className="sticky top-0 z-10 flex items-center justify-between border-b border-line bg-surface/95 px-7 py-5 backdrop-blur">
              <div>
                <div className="eyebrow">{eyebrow}</div>
                <h2 className="mt-1 text-lg font-semibold">{title}</h2>
              </div>
              <button className="btn-ghost" onClick={onClose}>Close</button>
            </div>
            <div className="flex-1 space-y-7 px-7 py-6">{children}</div>
            <div className="border-t border-line px-7 py-4 text-xs text-faint">
              Keys: 1-4 screens · G gateway · H how it works · R reset · F recorded copy
            </div>
          </motion.aside>
        </>
      )}
    </AnimatePresence>,
    document.body,
  );
}

/** The per-screen "How it works" drawer: panels, optional facts, optional extra sections. */
export function HowItWorksDrawer({ hiw, children }: { hiw: HowItWorks; children?: ReactNode }) {
  const { drawerOpen, setDrawerOpen } = useShell();
  return (
    <DrawerFrame open={drawerOpen} onClose={() => setDrawerOpen(false)} eyebrow="For the technical audience" title="How it works">
      <Panels hiw={hiw} />
      {children && <div className="space-y-7 border-t border-line pt-7">{children}</div>}
    </DrawerFrame>
  );
}

export function Panels({ hiw }: { hiw: HowItWorks }) {
  return (
    <>
      {hiw.panels.map((p) => (
        <section key={p.title}>
          <h3 className="font-semibold">{p.title}</h3>
          <p className="mt-1.5 text-sm leading-relaxed text-muted">{p.body}</p>
          {p.products.length > 0 && <ProductChips products={p.products} className="mt-3" />}
        </section>
      ))}
      {hiw.facts && hiw.facts.length > 0 && <KeyValue items={hiw.facts} />}
    </>
  );
}

export function ProductChips({ products, className = "" }: { products: string[]; className?: string }) {
  return (
    <div className={`flex flex-wrap gap-1.5 ${className}`}>
      {products.map((p) => (
        <span key={p} className="rounded-md border border-action/25 bg-action/[0.07] px-2 py-0.5 text-xs text-action">{p}</span>
      ))}
    </div>
  );
}

export function Section({ title, children, note }: { title: string; children: ReactNode; note?: string }) {
  return (
    <section>
      <div className="eyebrow mb-3">{title}</div>
      {children}
      {note && <p className="mt-2 text-xs text-faint">{note}</p>}
    </section>
  );
}

export function KeyValue({ items }: { items: [string, ReactNode][] }) {
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-2 text-sm">
      {items.map(([k, v]) => (
        <div key={k} className="contents">
          <dt className="text-muted">{k}</dt>
          <dd className="tnum text-ink/90">{v}</dd>
        </div>
      ))}
    </dl>
  );
}
