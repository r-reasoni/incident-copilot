# Incident Copilot

> **Cheap, real-time anomaly detection on application logs — LLM investigation only when it matters.**

---

## Overview

Incident Copilot ingests raw application logs, extracts log templates with **Drain3**,
builds time-window feature vectors, scores them online with **River HalfSpaceTrees**,
merges anomalous windows into *incidents*, and only then hands control to an **LLM agent**
that explains the probable root cause and proposes ranked remediation steps.

The LLM is never called speculatively — it fires only on confirmed incidents, keeping
costs and latency minimal.

---

## Repository Layout

```
incident-copilot/
├── app/
│   ├── api/          # FastAPI routers & OpenAPI spec
│   ├── ingest/       # Log ingestion: file tail, syslog UDP, HTTP push
│   ├── parsing/      # Drain3 wrapper → log templates + variables
│   ├── detection/    # River HalfSpaceTrees online anomaly scorer
│   ├── incidents/    # Incident CRUD, merge logic, Postgres models
│   ├── agent/        # LLM agent: provider wrapper, tools, prompts
│   └── db/           # SQLAlchemy engine / session factory, schema init
├── cli/              # Typer CLI (ingest, investigate, list-incidents …)
├── testbed/          # Synthetic log generators & replay utilities
├── eval/             # Precision/recall harness, LLM grader
├── tests/            # pytest suite
├── docs/
│   └── SPEC.md       # Detailed pipeline specification
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── .env.example
```

---

## Quick Start

```bash
# 1. Clone & enter
git clone <repo> incident-copilot && cd incident-copilot

# 2. Create a virtual environment (Python 3.13)
python -m venv .venv
# PowerShell: .\.venv\Scripts\Activate.ps1
# macOS/Linux: source .venv/bin/activate

# 3. Install all dependencies (including dev extras)
pip install -e ".[dev]"

# 4. Configure environment
Copy-Item .env.example .env  # PowerShell
# Edit .env and replace GEMINI_API_KEY=your_key_here with your key

# 5. Start PostgreSQL (works without Make)
docker compose up -d --wait

# 6. Initialize database schema
python -m app.db.init

# 7. Run tests
pytest

# 8. Start the API server
copilot serve --port 8000

# Stop PostgreSQL when finished
docker compose down
```

---

## CLI Usage (`copilot`)

After `pip install -e ".[dev]"`, the `copilot` command is available:

```bash
copilot --help                              # Show all commands
copilot init-db                             # Initialize PostgreSQL schema
copilot serve --port 8000                   # Start FastAPI + uvicorn server
copilot replay testbed/logs/api-service.log --mock --speed 0
                                            # Replay a plain log file in-process (event time)
copilot incidents [--status analyzed]       # Rich table of incidents
copilot show <id>                           # Rich panel with root cause, evidence, alternatives & remediation
copilot stats                               # Counts + % LLM calls saved vs per-log and per-window
copilot watch [--url ws://localhost:8000/ws/alerts]
                                            # Live alert stream (auto-reconnects)
copilot demo --fault cpu_hog --fast         # End-to-end live demo (~5 min with --fast)
copilot eval --runs 3 --mock                # Evaluation harness (Step 9)
```

The commands above are the direct commands for Windows PowerShell. The equivalent
Make targets (`make up`, `make down`, `make test`, and `make run-api`) are optional
convenience commands and require GNU Make to be installed.

The FastAPI interactive docs will be at <http://localhost:8000/docs>.

---

## Makefile Targets

| Target | Plain Command (PowerShell / Bash) | Description |
|---|---|---|
| `make up` | `docker compose up -d --wait` | Start PostgreSQL via Docker Compose |
| `make down` | `docker compose down` | Stop containers (data preserved) |
| `make down-volumes` | `docker compose down -v` | Stop containers AND destroy Postgres volume |
| `make logs` | `docker compose logs -f` | Tail container logs |
| `make install` | `pip install -e ".[dev]"` | Install Python deps into active venv |
| `make migrate` | `python -m app.db.init` | Initialize PostgreSQL schema |
| `make test` | `pytest` | Run the full pytest suite |
| `make test-cov` | `pytest --cov=app --cov-report=html --cov-report=term-missing` | Run tests with HTML coverage report |
| `make lint` | `ruff check .` | Run `ruff check` |
| `make format` | `ruff format .` | Run `ruff format` |
| `make typecheck` | `mypy app cli` | Run `mypy` |
| `make run-api` | `uvicorn app.api.main:app --reload` | Start FastAPI dev server with auto-reload |
| `make clean` | `find . -type d -name "__pycache__" -exec rm -rf {} +` | Remove cache dirs and test artifacts |

---

## Configuration

All settings are read from environment variables (or `.env`).
See [`.env.example`](.env.example) for the full list with descriptions.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot` | Postgres connection string |
| `LLM_PROVIDER` | `gemini` | `gemini` or `mock` |
| `GEMINI_API_KEY` | `your_key_here` | Gemini API key; required only when a Gemini call is made |
| `GEMINI_MODEL` | `gemini-3.8-flash` | Model name forwarded to the SDK |
| `WINDOW_SECONDS` | `10` | Feature-extraction time window in seconds |
| `WARMUP_WINDOWS` | `30` | Number of initial windows before scoring is trusted |
| `ANOMALY_GAP_SECONDS` | `30` | Max gap to merge adjacent anomalous windows |
| `TOP_K_TEMPLATES` | `30` | Number of top templates tracked |
| `WINDOW_GRACE_SECONDS` | `5` | Grace period in seconds for late-arriving events |
| `HST_N_TREES` | `25` | Number of trees in HalfSpaceTrees online detector |
| `HST_HEIGHT` | `15` | Depth/height of each tree in HalfSpaceTrees |
| `HST_WINDOW_SIZE` | `100` | Window size for River HalfSpaceTrees |
| `THRESHOLD_LOOKBACK` | `200` | Past window scores lookback for dynamic threshold |
| `THRESHOLD_STD_MULT` | `3.0` | Standard deviation multiplier for anomaly threshold |
| `CONSECUTIVE_HIGH_WINDOWS` | `2` | Consecutive anomalous windows required for incident |
| `INCIDENT_COOLDOWN_SECONDS` | `60` | Cooldown period before creating a new incident |
| `MAX_AGENT_TOOL_CALLS` | `5` | Max tool executions per LLM incident investigation |
| `DRAIN_STATE_PATH` | `data/drain_state.bin` | File path for persisting Drain3 parser state |
| `METRICS_SAMPLE_SECONDS` | `5` | Interval in seconds for sampling host metrics |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Web API | FastAPI + Uvicorn |
| Database | PostgreSQL 16 (Docker) + SQLAlchemy 2 |
| DB Driver | psycopg3 (`psycopg[binary]`) |
| Log parsing | Drain3 |
| Online ML | River – HalfSpaceTrees |
| System metrics | psutil |
| CLI | Typer |
| LLM backends | Google Gen AI (Gemini); Anthropic SDK available with `.[anthropic]` |
| Testing | pytest + pytest-asyncio + pytest-cov |
| Linting | ruff · mypy |

---

## Pipeline Summary

See [`docs/SPEC.md`](docs/SPEC.md) for the full specification.

```
Raw logs
  │  (tail / UDP / HTTP push)
  ▼
Drain3 parser          → log template + variable tokens
  │  (per line)
  ▼
Time-window aggregator → feature vector per window
  │  (rolling, non-overlapping)
  ▼
River HalfSpaceTrees   → anomaly score ∈ [0, 1]
  │  (online, no retraining needed)
  ▼
Incident merger        → merge consecutive anomalous windows → Incident (Postgres)
  │  (gap < INCIDENT_MERGE_GAP_SECONDS)
  ▼
LLM agent (on demand)  → root cause + confidence + remediation steps
  │  (tool-augmented: search logs, query metrics, check deploys)
  ▼
Human in the loop      → approve / reject recommended actions
```

---

## Development Notes

- **Never auto-execute remediation** — the agent proposes; humans decide.
- The LLM wrapper supports Gemini and a deterministic offline `mock` provider.
- `python scripts/check_llm.py` sends a short Gemini connectivity check without printing credentials.
- All database access goes through the session factory in `app/db/`;
  raw SQL is never used.
- The local testbed runs as plain Python processes with `python -m testbed.run_all`.
  Its `packet_loss` fault is an application-level simulation that delays selected
  requests until clients time out; it does not alter operating-system networking.

---

## License

MIT © 2024 Incident Copilot Contributors
