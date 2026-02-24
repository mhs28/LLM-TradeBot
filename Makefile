UV ?= uv

.PHONY: dev-api test docker-build docker-up docker-down docker-logs

dev-api:
	$(UV) run uvicorn llm_tradebot.services.api.main:app --reload --host 0.0.0.0 --port $${PORT:-8000}

test:
	$(UV) run pytest

docker-build:
	docker compose build

docker-up:
	docker compose up --build

docker-down:
	docker compose down --remove-orphans

docker-logs:
	docker compose logs -f --tail=200
