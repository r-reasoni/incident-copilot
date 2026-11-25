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
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 1 – Drain3 Online Log Parser        [app/parsing/]         │
│  • Template extraction via depth-limited parse tree               │
│  • Output: (template_id, template_str, variables[], timestamp)    │
└───────────────────┬───────────────────────────────────────────────┘
                    │ structured log events
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 2 – Time-Window Feature Extractor   [app/detection/]       │
│  • Non-overlapping windows of WINDOW_SECONDS seconds              │
│  • Feature vector per window (see §4)                             │
│  • Warm-up period: first WARMUP_WINDOWS windows skipped           │
└───────────────────┬───────────────────────────────────────────────┘
                    │ feature vectors
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 3 – River HalfSpaceTrees Scorer     [app/detection/]       │
│  • Online, one-class anomaly detection                            │
│  • Outputs score ∈ [0, 1]; higher → more anomalous                │
│  • Windows with score > dynamic threshold are "anomalous"         │
└───────────────────┬───────────────────────────────────────────────┘
                    │ (window, score, is_anomalous)
                    ▼
┌───────────────────────────────────────────────────────────────────┐
│  Stage 4 – Incident Merger                 [app/incidents/]       │
│  • Consecutive anomalous windows within ANOMALY_GAP_SECONDS       │
│    are merged into a single Incident record                       │
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
│    or REST API (GET /incidents/{id})                              │
│  • Operator chooses: acknowledge / dismiss / mark-resolved        │
│  • NO auto-execution of remediation steps                         │
└───────────────────────────────────────────────────────────────────┘
```

### 2.1 Pipeline Interface

The ingestion and execution runtime communicates with the processing stages through the `Pipeline` interface defined in `app/pipeline.py`:

```python
class Pipeline:
    def process(self, event: dict[str, Any]) -> None:
        """Ingest a single structured log event into the pipeline."""

    def tick(self, now: datetime) -> None:
        """Wall-clock or event-time heartbeat to close windows and incidents
        when no new events arrive."""

    def flush(self) -> None:
        """Close everything (end of replay / shutdown)."""
```

- **`process(event: dict[str, Any]) -> None`**: Called whenever a log event is ingested (via file tailing, syslog UDP, or HTTP POST `/ingest/logs`). In the current groundwork stage, it counts events by service. In subsequent stages, it passes events to Drain3 parsing and window feature extraction.
- **`tick(now: datetime) -> None`**: Provides an event-time or wall-clock heartbeat. It ensures that time windows and incidents are closed cleanly when log traffic stalls or stops. In production, a background task in the FastAPI lifespan calls `tick(now)` every 1 second. During offline replay, the testbed replayer emits ticks based on log event timestamps.
- **`flush() -> None`**: Shuts down the pipeline cleanly, closing all open windows, finalizing in-flight incidents, and saving parser/model state (e.g. Drain3 state to disk). Called during FastAPI lifespan shutdown and at the end of benchmark replay sessions.

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
| `detected_at` | `TIMESTAMPTZ` | Nullable detection / confirmation timestamp |
| `ended_at` | `TIMESTAMPTZ` | Nullable until the incident ends |
| `analyzed_at` | `TIMESTAMPTZ` | Nullable LLM analysis completion timestamp |
| `status` | `TEXT NOT NULL` | `open`, `analyzed`, or `closed` |
| `category` | `TEXT` | Nullable fault category |
| `peak_score` | `FLOAT NOT NULL` | |
| `affected_services` | `TEXT[] NOT NULL` | |
| `top_templates` | `JSONB NOT NULL` | |
| `alternatives` | `JSONB` | Nullable alternative root cause hypotheses |
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

### 3.6 `metrics_samples`

Host and service metrics samples collected periodically from target services.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL PK` | |
| `ts` | `TIMESTAMPTZ NOT NULL` | Sample timestamp |
| `service` | `TEXT NOT NULL` | Target service name |
| `cpu_percent` | `FLOAT NOT NULL` | Process/container CPU utilization % |
| `mem_percent` | `FLOAT NOT NULL` | Memory utilization % |
| `open_handles` | `INT NOT NULL` | Number of open file descriptors/handles |
| `threads` | `INT NOT NULL` | Thread count |
| `p50_ms` | `FLOAT NOT NULL` | 50th percentile response latency in ms |
| `p95_ms` | `FLOAT NOT NULL` | 95th percentile response latency in ms |
| `error_count` | `INT NOT NULL` | Count of service errors |
| `in_flight_requests` | `INT NOT NULL` | Current in-flight request count |
| `request_count_30s` | `INT NOT NULL` | Requests served in the last 30s |
| `probe_ms` | `FLOAT` | Nullable health probe latency in ms |
| `probe_ok` | `BOOLEAN NOT NULL` | Health probe success flag |

Indexes: `(service, ts)`.

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
- **LLM Grade** – root-cause accuracy evaluated against ground-truth fault types

### 8.1 Live vs. Replay Evaluation Policy

**Official Policy**: Official evaluation metrics and benchmark numbers come strictly from replaying a recorded dataset (`eval.replay`); live runs (`eval.run_step7`) are intended for real-time demonstration and end-to-end integration validation.

**Why Live and Replay Differ**:
- **Scheduling Jitter & Network Latency**: In live runs, testbed service logging, HTTP transport over sockets, OS thread scheduling, and background cron or garbage collection introduce variable delays (tens to hundreds of milliseconds). Log events occurring near window boundaries (e.g. at 09.98s vs 10.02s) may straddle different 10-second aggregation windows in live runs compared to their exact event timestamps.
- **Deterministic Event-Time Evaluation**: Replay processes events strictly according to their recorded `event["ts"]` timestamps, advancing the pipeline clock deterministically. Two runs of `eval.replay` over the exact same log file produce 100% identical window scores, thresholds, flags, and incident groupings.
- **The `memory_leak` Divergence (Case Study)**: In the Step 5 benchmark run, `memory_leak` produced 0 anomalous flags during the live run, but 1 anomalous flag during offline replay. In the live run, logging transport jitter split the cluster of memory-leak warning logs across two consecutive wall-clock windows (diluting the log rate and feature mass below the anomaly threshold). In replay, event-time alignment placed the burst into its true 10-second event interval, elevating the feature vector and crossing the threshold. Because ground truth is based on the system's actual events, deterministic event-time replay represents the true evaluation standard.

### 8.2 Replay Truncation & Harness Artifact Prevention

- **Loadgen Synchronization**: In `eval/run_step7.py`, traffic generation (`loadgen`) runs continuously until the post-fault observation window finishes (last fault end + 150 seconds). Both load generation and log recording terminate concurrently so that abrupt traffic drops do not generate false positive incidents.
- **Replay Truncation Default**: In `eval/replay.py`, `--truncate-after-faults` defaults to 120 seconds after the final fault ends. Events logged beyond this 120-second recovery boundary represent testbed teardown and process exit artifacts, which are excluded from offline benchmark evaluations.
- **Per-Provider Agent Output**: In `eval/replay.py`, `--agent-out <path>` writes structured per-incident agent diagnostics (incident metadata, status, model used, tool calls, category, confidence, root cause, remediation, and errors) without ground-truth fault labels to decouple analysis recording from evaluation scoring.

---

## 9. Configuration Reference

All settings are read from environment variables or `.env`. Below is the complete reference of configuration options:

| Setting | Type | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | `str` | `postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot` | PostgreSQL connection string |
| `LLM_PROVIDER` | `str` | `gemini` | LLM backend provider (`gemini` or `mock`) |
| `GEMINI_API_KEY` | `str` | `your_key_here` | API key for Gemini; evaluated only upon making LLM calls |
| `GEMINI_MODEL` | `str` | `gemini-3.8-flash` | Gemini primary model identifier |
| `GEMINI_FALLBACK_MODELS` | `str` | `""` | Comma-separated list of fallback models when primary fails |
| `WINDOW_SECONDS` | `int` | `10` | Duration of each aggregation time window in seconds |
| `WARMUP_WINDOWS` | `int` | `30` | Initial baseline windows observed before scoring is trusted |
| `ANOMALY_GAP_SECONDS` | `int` | `30` | Maximum interval to merge adjacent anomalous windows into one incident |
| `TOP_K_TEMPLATES` | `int` | `30` | Number of most frequent log templates tracked in state/features |
| `WINDOW_GRACE_SECONDS` | `int` | `5` | Grace period in seconds to accept out-of-order log events before closing window |
| `HST_N_TREES` | `int` | `25` | Number of trees in River HalfSpaceTrees online anomaly detector |
| `HST_HEIGHT` | `int` | `15` | Maximum depth of trees in HalfSpaceTrees ensemble |
| `HST_WINDOW_SIZE` | `int` | `100` | Sliding window size parameter for HalfSpaceTrees |
| `THRESHOLD_LOOKBACK` | `int` | `200` | Number of historical window scores used to compute dynamic threshold |
| `THRESHOLD_STD_MULT` | `float` | `3.0` | Multiplier for standard deviation over rolling mean for threshold |
| `THRESHOLD_CEILING` | `float` | `0.98` | Maximum cap on dynamic anomaly threshold (scores bounded in [0, 1]) |
| `SUSPECT_WINDOWS` | `int` | `3` | Quarantine window count following any high score to guard against threshold poisoning |
| `CONSECUTIVE_HIGH_WINDOWS` | `int` | `2` | Number of consecutive anomalous windows required to open an incident |
| `INCIDENT_COOLDOWN_SECONDS` | `int` | `60` | Cooldown period before a new incident can open for the same service |
| `MAX_AGENT_TOOL_CALLS` | `int` | `5` | Maximum number of tool iterations permitted per incident investigation |
| `DRAIN_STATE_PATH` | `str` | `data/drain_state.bin` | File path for persisting Drain3 parse tree state |
| `METRICS_SAMPLE_SECONDS` | `int` | `5` | Interval in seconds between background host metric samples |
| `RECORD_EVENTS_PATH` | `str` | `""` | Optional file path to stream all ingested raw events as JSON lines |
| `RECORD_METRICS_PATH` | `str` | `""` | Optional file path to stream all metrics samples as JSON lines |
| `RECORD_WINDOWS_PATH` | `str` | `""` | Optional file path to stream all scored window results as JSON lines |

### 9.1 Effective HST Window Size & Suspect Quarantine

- **Effective HST Window Size**: HalfSpaceTrees scores are not meaningful until the initial reference tree mass distributions are filled. To guarantee this occurs cleanly during warm-up regardless of configuration, the effective window size is calculated as:
  $$\text{effective\_hst\_window} = \min(\text{HST\_WINDOW\_SIZE}, \max(5, \lfloor \text{WARMUP\_WINDOWS} / 2 \rfloor))$$
- **Suspect-Window Quarantine**: When a window scores at or above the threshold ($\text{score} \ge \text{threshold}$), the following $\text{SUSPECT\_WINDOWS}$ windows are quarantined. They are neither learned by the MinMaxScaler / HalfSpaceTrees nor added to the rolling threshold lookback history, even if their scores fall slightly below the threshold. This prevents threshold poisoning from lingering or fluctuating failure modes.
- **Time-to-Detect & Confirmation**: Each window result provides `confirmed_at` (set on the exact window where consecutive-high requirements are first satisfied, equal to `window_end`) and `run_start` (equal to the starting timestamp of the initial high window in the run).

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
