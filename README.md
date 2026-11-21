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

# 6. Apply database migrations
make migrate

# 7. Run tests
pytest

# 8. Start the API server
uvicorn app.api.main:app --reload

# Stop PostgreSQL when finished
docker compose down
```

The commands above are the direct commands for Windows PowerShell. The equivalent
Make targets (`make up`, `make down`, `make test`, and `make run-api`) are optional
convenience commands and require GNU Make to be installed.

The FastAPI interactive docs will be at <http://localhost:8000/docs>.

---

## Makefile Targets

| Target | Description |
|---|---|
| `make up` | Start PostgreSQL via Docker Compose |
| `make down` | Stop containers (data preserved) |
| `make install` | Install Python deps into active venv |
| `make test` | Run the full pytest suite |
| `make test-cov` | Run tests with HTML coverage report |
| `make run-api` | Start FastAPI dev server with auto-reload |
| `make migrate` | Run Alembic `upgrade head` |
| `make lint` | Run `ruff check` |
| `make format` | Run `ruff format` |
| `make typecheck` | Run `mypy` |
| `make clean` | Remove cache dirs |

---

## Configuration

All settings are read from environment variables (or `.env`).
See [`.env.example`](.env.example) for the full list with descriptions.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot` | Postgres connection string |
| `LLM_PROVIDER` | `gemini` | `gemini` or `mock` |
| `GEMINI_API_KEY` | `your_key_here` | Gemini API key; required only when a Gemini call is made |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Model name forwarded to the SDK |
| `WINDOW_SECONDS` | `10` | Feature-extraction time window |
| `WARMUP_WINDOWS` | `30` | Windows before scoring is trusted |
| `ANOMALY_GAP_SECONDS` | `30` | Max gap to merge adjacent anomalous windows |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Web API | FastAPI + Uvicorn |
| Database | PostgreSQL 16 (Docker) + SQLAlchemy 2 + Alembic |
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
