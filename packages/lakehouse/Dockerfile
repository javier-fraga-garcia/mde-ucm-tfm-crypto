FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV UV_NO_DEV=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/shared ./packages/shared
COPY packages/lakehouse ./packages/lakehouse

RUN uv sync --frozen --package lakehouse

RUN groupadd --gid 1000 lakehouse && \
    useradd --uid 1000 --gid lakehouse --shell /bin/bash --create-home lakehouse && \
    chown -R lakehouse:lakehouse /app

USER lakehouse

CMD ["uv", "run", "python", "-m", "lakehouse.jobs.bronze_job"]