FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml README.md ./
COPY llm_tradebot ./llm_tradebot

RUN uv pip install --system --no-cache-dir .

FROM base AS api

EXPOSE 8000

CMD ["sh", "-c", "python -m uvicorn llm_tradebot.services.api.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
