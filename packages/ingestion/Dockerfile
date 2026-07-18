FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_NO_DEV=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/shared ./packages/shared
COPY packages/ingestion ./packages/ingestion

RUN uv sync --frozen --package ingestion

RUN groupadd --gid 1000 ingestion && \
    useradd --uid 1000 --gid ingestion --shell /bin/bash --create-home ingestion && \
    chown -R ingestion:ingestion /app

USER ingestion

CMD ["uv", "run", "python", "-m", "ingestion.cli"]