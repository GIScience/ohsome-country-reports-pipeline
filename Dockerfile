# --- tippecanoe builder ---
FROM debian:bookworm-slim AS tippecanoe-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsqlite3-dev \
    zlib1g-dev \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG TIPPECANOE_VERSION=2.79.0
RUN git clone --depth 1 --branch ${TIPPECANOE_VERSION} \
    https://github.com/felt/tippecanoe.git /tippecanoe
WORKDIR /tippecanoe
RUN make -j"$(nproc)" && make install PREFIX=/usr/local

# --- main image ---
FROM python:3.14-slim

# tippecanoe's runtime deps (no build toolchain needed here)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=tippecanoe-builder /usr/local/bin/tippecanoe /usr/local/bin/tippecanoe
COPY --from=tippecanoe-builder /usr/local/bin/tippecanoe-decode /usr/local/bin/tippecanoe-decode
COPY --from=tippecanoe-builder /usr/local/bin/tile-join /usr/local/bin/tile-join

COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .
COPY README.md .

RUN mkdir --parents /data/dagster/home
RUN mkdir --parents /data/dagster/data

COPY dagster.yaml /data/dagster/home/

RUN uv sync --frozen --no-install-project

COPY ./src /app/src

RUN uv sync --frozen

ENV PATH="/app/.venv/bin:$PATH"

CMD ["dagster", "dev", "-p", "3000"]