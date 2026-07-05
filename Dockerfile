# Hosted/self-hosted executor strategy-runtime image.
#
# Build context is the REPO ROOT (so we can pull in ../strategy-library
# alongside strategy-service/). Build with:
#
#   docker build -f strategy-service/Dockerfile \
#     --target executor \
#     -t hushine/strategy-runtime:executor-dev \
#     /home/xdy/Workplace/hushine
#
# (or use scripts/build_strategy_runtime.sh which handles this for you.)
#
# Hosted and self-hosted runtimes share the same RuntimeChannel entrypoint.
# Hosted containers receive an internal credential from control-panel; local
# debug can use `hushine-runtime start --user-id ...` only when the
# control-panel debug gate is enabled.

FROM python:3.13-slim AS runtime-base

# System deps for psycopg2-binary + grpcio compile-free wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Project metadata first so dependency resolution caches across code edits.
COPY strategy-service/pyproject.toml /app/strategy-service/pyproject.toml
COPY strategy-service/uv.lock /app/strategy-service/uv.lock
RUN pip install --no-cache-dir uv

# strategy-library is a sibling directory; the in-source `strategy-library/`
# symlink points to it. Copy the target so the image doesn't need symlink
# resolution at runtime.
COPY strategy-library/ /app/strategy-library/

# strategy-service code (everything except the dangling symlink).
COPY strategy-service/hushine_runtime_cli.py /app/strategy-service/hushine_runtime_cli.py
COPY strategy-service/strategy_service/ /app/strategy-service/strategy_service/
COPY strategy-service/strategy_templates/ /app/strategy-service/strategy_templates/
COPY strategy-service/proto/ /app/strategy-service/proto/
COPY strategy-service/config.yaml /app/strategy-service/config.yaml

# In the image we recreate the symlink layout the source tree uses so
# imports like `from utils.log import ...` resolve via PYTHONPATH the
# same way local dev does.
RUN ln -s /app/strategy-library /app/strategy-service/strategy-library \
    && cd /app/strategy-service \
    && uv sync --frozen --no-dev

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV="/app/strategy-service/.venv" \
    PATH="/app/strategy-service/.venv/bin:$PATH" \
    PYTHONPATH="/app/strategy-service:/app/strategy-library"

WORKDIR /app/strategy-service

FROM runtime-base AS executor

ENV HUSHINE_RUNTIME_ROLE=executor

CMD ["uv", "run", "--no-sync", "python", "-m", "hushine_runtime_cli", "start", "--config", "config.yaml"]

FROM executor AS default
