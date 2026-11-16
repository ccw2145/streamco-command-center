import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Neutral, dark, projector-friendly surfaces.
        canvas: "#0A0D14",
        surface: "#111622",
        raised: "#171D2B",
        line: "#232B3D",
        ink: "#EEF1F7",
        muted: "#9AA3B5",
        faint: "#646D80",
        // Interactive (links, focus, the one primary button).
        action: "#8FB3FF",
        // Healthy / passed.
        ok: "#4FD1A5",
        // THE accent: alerts only.
        alert: "#FF6B57",
      },
      fontFamily: {
        sans: ['"Inter Variable"', "Inter", "system-ui", "sans-serif"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      fontSize: {
        // Large numeric scale for tiles and headlines.
        "num-sm": ["2rem", { lineHeight: "1.05", letterSpacing: "-0.02em", fontWeight: "650" }],
        "num-md": ["2.75rem", { lineHeight: "1", letterSpacing: "-0.03em", fontWeight: "650" }],
        "num-lg": ["3.75rem", { lineHeight: "1", letterSpacing: "-0.035em", fontWeight: "650" }],
        "num-xl": ["4.75rem", { lineHeight: "0.95", letterSpacing: "-0.04em", fontWeight: "650" }],
        headline: ["2.125rem", { lineHeight: "1.15", letterSpacing: "-0.02em", fontWeight: "600" }],
      },
      borderRadius: {
        card: "18px",
      },
      boxShadow: {
        card: "0 1px 0 0 rgba(255,255,255,0.03) inset, 0 12px 32px -18px rgba(0,0,0,0.6)",
        drawer: "-24px 0 48px -16px rgba(0,0,0,0.6)",
      },
      keyframes: {
        shimmer: { "0%": { backgroundPosition: "-400px 0" }, "100%": { backgroundPosition: "400px 0" } },
        pulsering: {
          "0%": { transform: "scale(1)", opacity: "0.7" },
          "100%": { transform: "scale(2.6)", opacity: "0" },
        },
      },
      animation: {
        shimmer: "shimmer 1.4s linear infinite",
        pulsering: "pulsering 1.6s ease-out infinite",
      },
    },
  },
  plugins: [],
} satisfies Config;
