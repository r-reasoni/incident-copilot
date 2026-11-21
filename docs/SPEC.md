# Incident Copilot – Pipeline Specification

**Version:** 0.1 (skeleton)
**Status:** Living document — updated with every implementation prompt.

---

## 1. Goals

| **Low cost** | The LLM is called *only* when an incident is confirmed — not for every log line or every minute. |
| **Explainability** | Every incident gets a structured root-cause + confidence + remediation list. |
| **Human-in-the-loop** | The agent proposes actions; nothing is auto-executed. |
| **Provider agnosticism** | Switching Gemini ↔ Anthropic requires only a `.env` change. |

---

## 2. High-Level Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                        Log Sources                                │
│   (file tail · syslog UDP · HTTP POST /ingest)                    │
└───────────────────┬───────────────────────────────────────────────┘
                    │ raw log lines (text)

                    | **Low cost**
│  • Template extraction via depth-limited parse tree               │
│  • Output: (template_id, template_str, variables[], timestamp)    │
└───────────────────┬───────────────────────────────────────────────┘
                    │ structured log events
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 2 – Time-Window Feature Extractor   [app/detection/]       │
│  • Non-overlapping windows of WINDOW_SIZE_SECONDS seconds         │
│  • Feature vector per window (see §4)                             │
│  • Warm-up period: first ANOMALY_WARMUP_WINDOWS windows skipped   │
└───────────────────┬───────────────────────────────────────────────┘
                    │ feature vectors
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 3 – River HalfSpaceTrees Scorer     [app/detection/]       │
│  • Online, one-class anomaly detection                             │
│  • Outputs score ∈ [0, 1]; higher → more anomalous                │
│  • Windows with score > ANOMALY_THRESHOLD are "anomalous"         │
└───────────────────┬───────────────────────────────────────────────┘
                    │ (window, score, is_anomalous)
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 4 – Incident Merger                 [app/incidents/]       │
│  • Consecutive anomalous windows within INCIDENT_MERGE_GAP_SECONDS│
│    are merged into a single Incident record                        │
│  • Incidents are persisted to PostgreSQL                          │
│  • Status lifecycle: OPEN → INVESTIGATING → RESOLVED / DISMISSED  │
└───────────────────┬───────────────────────────────────────────────┘
                    │ Incident created / updated event
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 5 – LLM Investigation Agent        [app/agent/]            │
│  • Triggered automatically for every new OPEN incident            │
│  • Provider-agnostic wrapper (Gemini or Anthropic)                │
│  • Agent tools (see §5)                                           │
│  • Produces: root_cause, confidence ∈ [0,1], remediation[]        │
│  • Result stored back on the Incident row                         │
└───────────────────┬───────────────────────────────────────────────┘
                    │ investigation result
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 6 – Human in the Loop                                      │
│  • Operator reviews via CLI (`incident-copilot investigate <id>`) │
│    or REST API (GET /incidents/{id})                               │
│  • Operator chooses: acknowledge / dismiss / mark-resolved        │
│  • NO auto-execution of remediation steps                         │
└───────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Model (PostgreSQL)

### 3.1 `log_events`

Stores timestamped raw and parsed log entries. In production, this high-volume
table could be time-partitioned; partitioning is not implemented.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL PK` | |
| `ts` | `TIMESTAMPTZ NOT NULL` | Event timestamp |
| `service` | `TEXT NOT NULL` | |
| `host` | `TEXT NOT NULL` | |
| `level` | `TEXT NOT NULL` | Parsed level |
| `raw_message` | `TEXT NOT NULL` | Original log message |
| `template_id` | `INT` | Nullable template reference |
| `params` | `JSONB` | Nullable extracted parameters |

Indexes: `(ts)`, `(service, ts)`, `(template_id)`.

### 3.2 `templates`

| Column | Type | Notes |
|---|---|---|
| `template_id` | `INT PK` | |
| `template_text` | `TEXT NOT NULL` | |
| `first_seen` | `TIMESTAMPTZ NOT NULL` | |
| `count` | `BIGINT NOT NULL` | Accumulated occurrence count |

### 3.3 `window_scores`

One row per scored time window; indexed by `window_start`.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL PK` | |
| `window_start` | `TIMESTAMPTZ NOT NULL` | |
| `window_end` | `TIMESTAMPTZ NOT NULL` | |
| `score` | `FLOAT NOT NULL` | Anomaly score |
| `is_anomalous` | `BOOLEAN NOT NULL` | |
| `features` | `JSONB NOT NULL` | Feature-name to value mapping |

### 3.4 `incidents`

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL PK` | |
| `started_at` | `TIMESTAMPTZ NOT NULL` | |
| `ended_at` | `TIMESTAMPTZ` | Nullable until the incident ends |
| `status` | `TEXT NOT NULL` | `open`, `analyzed`, or `closed` |
| `peak_score` | `FLOAT NOT NULL` | |
| `affected_services` | `TEXT[] NOT NULL` | |
| `top_templates` | `JSONB NOT NULL` | |
| `root_cause` | `TEXT` | Nullable LLM analysis |
| `confidence` | `FLOAT` | Nullable LLM confidence |
| `remediation` | `JSONB` | Nullable remediation list |
| `agent_trace` | `JSONB` | Nullable agent trace |
| `llm_calls` | `INT NOT NULL DEFAULT 0` | |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

### 3.5 `fault_events`

Ground-truth events written by the fault injector and used only for evaluation.
Fault events are never exposed to the investigation agent.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL PK` | |
| `fault_type` | `TEXT NOT NULL` | |
| `target_service` | `TEXT NOT NULL` | |
| `started_at` | `TIMESTAMPTZ NOT NULL` | |
| `ended_at` | `TIMESTAMPTZ` | Nullable while a fault is active |

---

## 4. Feature Vector Specification

Each time window produces the following features (all `float`):

| Feature | Description |
|---|---|
| `log_count` | Total log lines in the window |
| `error_rate` | Fraction of lines with level ERROR or CRITICAL |
| `warn_rate` | Fraction of lines with level WARN |
| `unique_templates` | Number of distinct Drain3 template IDs |
| `template_entropy` | Shannon entropy of the template-ID distribution |
| `lines_per_second` | Throughput = log_count / window_size |
| `cpu_pct` | Host CPU % at window close (via psutil) |
| `mem_pct` | Host RSS memory % at window close (via psutil) |

Additional features may be added per prompt; the vector is stored as JSONB
so the schema never needs migration for new features.

---

## 5. LLM Agent Tools

The agent is given these tools to investigate an incident:

| Tool | Input | Output |
|---|---|---|
| `get_incident_windows` | `incident_id` | List of scored windows (feature vectors + scores) |
| `search_logs` | `incident_id, regex, limit` | Matching raw log lines from the incident's time range |
| `get_template_counts` | `incident_id` | Template ID → count histogram for the incident |
| `get_host_metrics` | `incident_id` | CPU / memory time series from stored windows |
| `get_recent_deploys` | `since, until` | Placeholder – returns empty list until wired to a CI system |

The agent runs in a **tool-loop** until it has enough information to produce
a final answer structured as:

```json
{
  "root_cause": "...",
  "confidence": 0.85,
  "remediation": [
    {"rank": 1, "action": "...", "rationale": "..."},
    {"rank": 2, "action": "...", "rationale": "..."}
  ]
}
```

---

## 6. REST API Endpoints (planned)

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest` | Push one or more raw log lines |
| `GET` | `/incidents` | List incidents (filterable by status, date range) |
| `GET` | `/incidents/{id}` | Retrieve a single incident with LLM analysis |
| `PATCH` | `/incidents/{id}/status` | Update status (acknowledge / resolve / dismiss) |
| `GET` | `/windows` | List scored windows (filterable by incident, time range) |
| `GET` | `/health` | Liveness check |

---

## 7. CLI Commands (planned)

```
incident-copilot ingest   <file>          Stream a log file through the pipeline
incident-copilot tail     <file>          Continuously tail and process a log file
incident-copilot list                     List recent incidents
incident-copilot show     <id>            Show full incident detail + LLM analysis
incident-copilot resolve  <id>            Mark an incident as RESOLVED
incident-copilot dismiss  <id>            Mark an incident as DISMISSED
incident-copilot replay   <file>          Replay a log dump at configurable speed (testbed)
incident-copilot eval     <labeled-file>  Run evaluation harness against labelled data
```

---

## 8. Evaluation Harness (`eval/`)

Labelled test sets contain log dumps with ground-truth incident time ranges.
The harness measures:

- **Precision** – what fraction of detected incidents are real
- **Recall** – what fraction of real incidents are detected
- **Mean Time To Detect (MTTD)** – lag from incident start to first anomalous window
- **LLM Grade** – GPT-4 or Gemini judges the quality of root-cause narratives (0–10)

---

## 9. Configuration Reference

See [`.env.example`](../.env.example) and [`app/config.py`](../app/config.py) for the
authoritative list of environment variables and their defaults.

---

## 10. Non-Goals (explicitly out of scope)

- **Auto-remediation** – The system never executes commands autonomously.
- **Frontend UI** – All interaction is via CLI or REST API.
- **Multi-tenant isolation** – Single-tenant deployment assumed.
- **Log storage beyond the incident window** – Long-term retention is the
  operator's responsibility (ship to S3, Loki, etc.).

---

## 11. Open Questions / Future Work

- [ ] Should `log_events` be partitioned by month in Postgres for large deployments?
- [ ] Add a Redis-backed queue between ingestion and scoring for horizontal scaling?
- [ ] Support streaming logs via WebSocket on the API?
- [ ] Add a Slack / PagerDuty notification hook when a new incident opens?
