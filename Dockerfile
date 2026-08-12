FROM python:3.14-slim

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