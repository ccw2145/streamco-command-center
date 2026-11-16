// Payload shapes. Source of truth: scripts/export_fixture.py (screens) and app/backend/routers (actions).

export type Status3 = "ok" | "warn" | "alert";
export type ScreenId = "truth" | "forecast" | "ask" | "save" | "governance";

export interface HiwPanel {
  title: string;
  body: string;
  products: string[];
}

export interface HowItWorks {
  panels: HiwPanel[];
  facts?: [string, string][];
}

// ---------- One Truth
export interface SourceSystem {
  source_id: string;
  name: string;
  system_type: string;
  prod_connector: string;
  bronze_tables: string[];
  row_count: number;
}

export interface TeamCount {
  team: string;
  short: string;
  count: number;
  definition: string;
  metric_id: string;
  /** count minus the governed count */
  delta?: number;
  /** why this team's count differs from the governed one */
  bridge?: string | null;
}

export interface Tile {
  id: string;
  label: string;
  display: string;
  compare_label: string;
  compare_display: string;
  status: Status3;
  metric_id: string;
  next?: { to: string; label: string };
}

export interface CoreEntity {
  name: string;
  key: string;
  count: number;
  systems: number;
  unmatched_ids: number;
  label: string;
  metric_id: string;
}

export interface DataQuality {
  n_rules: number;
  rows_checked: number;
  pct_passed: number;
  quarantined: number;
  warned: number;
  failing: { rule: string; dataset: string; action: string; failed: number; checked: number }[];
  pipelines: string[];
  run_ts: string;
  status: "ok" | "warn";
  label: string;
  detail: string;
  metric_id: string;
}

export interface TruthPayload {
  story: string;
  badges: { id: string; label: string; detail: string }[];
  sources: SourceSystem[];
  pipeline: { layers: { id: string; label: string; summary: string }[]; metric_view: { name: string; measures: string[] } };
  team_counts: {
    tier: string;
    teams: TeamCount[];
    governed: { count: number; definition: string; metric_id: string };
    reconciliation: { ledger: number; pct_diff: number; status: string; label: string };
  };
  tiles: Tile[];
  core_entity?: CoreEntity | null;
  data_quality?: DataQuality | null;
  how_it_works: HowItWorks;
}

// ---------- Forecast
export interface Clock {
  t0: string;
  kickoff: string;
  halftime_start: string;
  halftime_end: string;
  end: string;
}

export interface SeriesPoint {
  t: string;
  actual: number;
  yhat: number;
  lo: number;
  hi: number;
}

export interface ForecastPayload {
  clock: Clock;
  series: SeriesPoint[];
  peak: { actual: number; forecast: number };
  callout: { cdn: string; metro: string; metro_name: string; util: number; detail: string };
  alert: {
    onset_t: string;
    detected_t: string;
    minutes_to_detect: number;
    headline: string;
    explanation: string;
    cohort_label: string;
    observed: number;
    baseline: number;
    affected_sessions: number;
    ops: { t: string; resolved_t: string; minutes_to_resolve: number; label: string };
  };
  how_it_works: HowItWorks;
}

// ---------- Ask
export interface LaneDef {
  lane: string;
  label: string;
  tools: string[];
  owner_system: string;
}

export interface LaneDone {
  lane: string;
  status: "ok" | "error";
  duration_ms: number;
  summary: string;
  sql_text: string | null;
  row_count: number;
}

export interface Driver {
  driver: "playback" | "payment" | "event_only" | "unexplained";
  label: string;
  pct: number;
  n_cancels: number;
  evidence_type: "observed" | "inferred" | "expected" | "unexplained";
  confidence: "high" | "medium" | "low";
  evidence: string;
  metric_id: string;
}

export interface Answer {
  question: string;
  headline: string;
  narrative: string;
  source?: string;
  declined: boolean;
  total?: { observed: number; expected: number; excess: number; pct_over: number; band_lo: number; band_hi: number };
  drivers: Driver[];
  reconciliation?: { status: string; subscriber_live: number; ledger: number; pct_diff: number } | null;
  drivers_sql?: string;
}

export type AskEvent =
  | { event: "lane_start"; data: LaneDef }
  | { event: "lane_done"; data: LaneDone }
  | { event: "answer"; data: Answer }
  | { event: "fallback"; data: { message: string } }
  | { event: "error"; data: { message: string } }
  | { event: "done"; data: { recorded: boolean } };

export interface AskPayload {
  primary_question: string;
  followup_suggestions: string[];
  lanes: LaneDef[];
  how_it_works: HowItWorks;
}

export interface GenieAnswer {
  question: string;
  conversation_id: string | null;
  text: string;
  declined: boolean;
  sql: string | null;
  columns: string[];
  rows: unknown[][];
  duration_ms: number;
  recorded: boolean;
}

// ---------- Save Subscribers
export interface PlanCard {
  card_id: string;
  title: string;
  who: string;
  offer: string;
  policy: string;
  policy_source?: string | null;
  n_subscribers: number;
  expected_cancels: number;
  save_rate: number;
  saved_point: number;
  saved_lo: number;
  saved_hi: number;
  cost_usd: number;
  revenue_protected_usd: number;
}

export interface Message {
  subject: string;
  body: string;
  watch_next: string;
  agent_note: string;
  model?: string;
}

export interface Sample {
  sample_id: string;
  subscriber_id?: number;
  card_id: string;
  first_name: string;
  metro_name: string;
  tier: string;
  device: string;
  tenure_months: number;
  what_happened: string;
  fav_genre: string;
  recent_titles: string[];
  message: Message;
}

export interface Approval {
  approved_by: string;
  approved_at: string;
  stored: "lakebase" | "memory";
  note: string | null;
  recorded: boolean;
  approval_id?: number | null;
}

/** GET /api/save/serving/{sample_id}: one subscriber read from the Lakebase synced tables. */
export interface Serving {
  source: "lakebase" | "recorded" | "unavailable";
  found?: boolean;
  ms?: number;
  table?: string;
  churn_prob?: number;
  risk_decile?: number;
  card_id?: string;
  top_drivers?: string[];
  model_version?: string;
  has_message?: boolean;
}

export interface ModelMonitoring {
  version: string;
  trained_at: string;
  auc_train: number;
  auc_recent: number;
  recent_window: string;
  recent_window_n: number;
  auc_drop: number;
  auc_by_window: { window: string; n: number; auc: number }[];
  scoring_snapshot: string;
  score_psi: number;
  max_top_psi: number;
  max_top_psi_feature: string;
  status: "ok" | "watch" | "retrain";
  status_reason: string;
  retrain_rule: string;
  retrain_short: string;
  challenger_version: string | null;
  monitored_at: string;
  features: { feature: string; label: string; rank: number; is_top: boolean; psi: number; baseline_mean: number | null;
    current_mean: number | null; drift: "low" | "moderate" | "high" }[];
  outcomes: { card_id: string; title: string; save_rate: number; plan_save_rate: number;
    tests: { test_id: string; run_date: string; n_treated: number; n_holdout: number; treated_rate: number;
      holdout_rate: number; lift: number }[] }[];
  metric_id: string;
}

export interface SavePayload {
  headline: {
    at_risk: number;
    projected_cancels_30d: number;
    saved_point: number;
    saved_lo: number;
    saved_hi: number;
    cost_usd: number;
    revenue_protected_usd: number;
    revenue_at_risk_usd: number;
  };
  model_card: {
    uc_name: string;
    alias: string;
    version: string;
    auc: number;
    scored: number;
    top_drivers: string[];
    monitoring?: ModelMonitoring | null;
  };
  cards: PlanCard[];
  samples: Sample[];
  run: { n_messages: number; cost_usd: number; model: string };
  assumptions: string;
  approval: Approval | null;
  how_it_works: HowItWorks;
}

export interface RegenerateResponse {
  sample_id: string;
  message: Message;
  recorded: boolean;
  note: string | null;
}

// ---------- Governed by Unity Gateway drawer
export interface GovernancePayload {
  seeded_note: string;
  usage: { calls: number; redactions: number; by_service: { service: string; calls: number }[]; costs: { label: string; usd: number }[] };
  live_usage: { calls: number; tokens: number; label: string } | null;
  budgets: { team: string; monthly_budget_usd: number; spend_usd: number; budget_pct: number; alert_pct: number; status: Status3 }[];
  masking: { masked_text: string; unmasked_text: string; n_redactions: number };
  trace: { label: string; url: string | null; steps: number; duration_s: number };
  how_it_works: HowItWorks;
}

export interface ScreenMap {
  truth: TruthPayload;
  forecast: ForecastPayload;
  ask: AskPayload;
  save: SavePayload;
  governance: GovernancePayload;
}

export interface Provenance {
  metric_id: string;
  label: string;
  sql: string;
  tables: string[];
  owner_system: string;
  columns: string[];
  rows: unknown[][];
  ran_at: string;
  lineage?: Lineage | null;
}

export interface Lineage {
  basis: string;
  url: string | null;
  hops: { layer: string; tables: string[]; basis: string }[];
}

export interface Health {
  live: boolean;
  force_fixture: boolean;
  illustrative: boolean;
  fixture_source: string;
  generated_at: string;
  gateway_model: string;
}
