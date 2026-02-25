# LLM Tradebot

Minimal, production-grade Python skeleton for a modular AI-agent-driven quant trading system.

## What this scaffold provides

- Python 3.12 package with strict modular boundaries.
- Shared `core` library for config, JSON logging, and common types.
- Independently runnable services under `llm_tradebot/services`.
- FastAPI smoke service with `/health` and `/version` endpoints.
- Docker Compose setup with one-command up/down and API healthcheck.
- `.env`-driven runtime configuration.

## Project layout

- `llm_tradebot/core`: shared config, logging, types, and time utils.
- `llm_tradebot/services/api`: smoke-check API service.
- `llm_tradebot/services/*`: runnable services for pipeline modules.
- `llm_tradebot/skills`: placeholder skill registry.
- `llm_tradebot/infra`: placeholder infra adapters.
- `tests`: minimal smoke test.
- `docker`: base and API Dockerfiles.

## Quickstart (local)

1. Copy env template:
   ```bash
   cp .env.example .env
   ```
2. Install deps and run API with uv:
   ```bash
   uv sync --dev
   uv run uvicorn llm_tradebot.services.api.main:app --reload
   ```
3. Verify:
   - `http://127.0.0.1:8000/health`
   - `http://127.0.0.1:8000/version`

## Quickstart (docker)

1. Copy env template:
   ```bash
   cp .env.example .env
   ```
2. Start all services:
   ```bash
   docker compose up --build
   ```
   API is available at `http://127.0.0.1:${API_PORT}` (default `18080`).
3. Stop:
   ```bash
   docker compose down
   ```

## Ingestor (Binance Futures WS)

Local:

```bash
export INGEST_SYMBOLS="ETHUSDT,BTCUSDT"
export INGEST_INTERVALS="5m,15m,1h"
export INGEST_WINDOW=200
uv run python -m llm_tradebot.services.ingestor
```

Docker:

```bash
docker compose up -d --build ingestor
docker compose logs -f ingestor
```

## Feature engine (Binance Futures WS snapshots)

The feature engine subscribes to closed candles for `5m,15m,1h` and writes one JSON
snapshot per line to `FEATURE_SNAPSHOT_PATH` (default `/app/data/feature_snapshots.jsonl`).

Local:

```bash
export FEATURE_SYMBOLS="ETHUSDT,BTCUSDT"
export FEATURE_INTERVALS="5m,15m,1h"
export FEATURE_WINDOW=200
export REGIME_TREND_THRESHOLD=0.002
export FEATURE_SNAPSHOT_PATH="./data/feature_snapshots.jsonl"
uv run python -m llm_tradebot.services.feature_engine
```

Docker:

```bash
docker compose up -d --build feature_engine
docker compose logs -f feature_engine
```

In Docker Compose, snapshots are persisted via `./data:/app/data`, so the default file is
available on the host at `./data/feature_snapshots.jsonl`.

## Router (deterministic JSONL decisions)

The router tails `ROUTER_SNAPSHOT_PATH` (default `/app/data/feature_snapshots.jsonl`) and
writes one JSON decision per line to `ROUTER_DECISION_PATH`
(default `/app/data/router_decisions.jsonl`).

Local:

```bash
export ROUTER_SNAPSHOT_PATH="./data/feature_snapshots.jsonl"
export ROUTER_DECISION_PATH="./data/router_decisions.jsonl"
export ROUTER_MIN_HOLD_BARS=3
export ROUTER_DEV_ENTRY_THRESHOLD=0.002
uv run python -m llm_tradebot.services.router
```

Docker:

```bash
docker compose up -d --build router
docker compose logs -f router
```

In Docker Compose, router data is persisted via `./data:/app/data`, so decisions are
available on the host at `./data/router_decisions.jsonl`.

## Make targets

- `make dev-api`: run API locally with reload.
- `make test`: run pytest smoke tests.
- `make docker-build`: build all container images.
- `make docker-up`: start compose stack.
- `make docker-down`: stop compose stack.
- `make docker-logs`: stream compose logs.
