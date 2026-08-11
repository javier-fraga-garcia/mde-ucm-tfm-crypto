FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_NO_DEV=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/shared ./packages/shared
COPY packages/serving ./packages/serving

RUN uv sync --frozen --package serving

RUN groupadd --gid 1000 serving && \
    useradd --uid 1000 --gid serving --shell /bin/bash --create-home serving && \
    chown -R serving:serving /app

USER serving

CMD ["uv", "run", "python", "-m", "serving.main"]