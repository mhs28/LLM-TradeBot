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
- `llm_tradebot/services/*`: runnable placeholders for future pipeline modules.
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
3. Stop:
   ```bash
   docker compose down
   ```

## Make targets

- `make dev-api`: run API locally with reload.
- `make test`: run pytest smoke tests.
- `make docker-build`: build all container images.
- `make docker-up`: start compose stack.
- `make docker-down`: stop compose stack.
- `make docker-logs`: stream compose logs.
