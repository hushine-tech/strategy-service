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
# bare runtimes connect with the Go runtime-agent after control-panel bootstrap.

FROM golang:1.26-bookworm AS go-builder-base

WORKDIR /src

COPY golang-lib/ /src/golang-lib/
COPY strategy-service/go.mod strategy-service/go.sum /src/strategy-service/
COPY strategy-service/cmd/ /src/strategy-service/cmd/
COPY strategy-service/gen/ /src/strategy-service/gen/
COPY strategy-service/internal/ /src/strategy-service/internal/

FROM go-builder-base AS go-builder

RUN cd /src/strategy-service \
    && go build -o /out/runtime-agent ./cmd/runtime-agent

FROM go-builder-base AS go-coverage-builder

RUN cd /src/strategy-service \
    && go build -cover -covermode=atomic -coverpkg=./... \
        -o /out/runtime-agent ./cmd/runtime-agent

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
COPY strategy-service/strategy_service/ /app/strategy-service/strategy_service/
COPY strategy-service/strategy_templates/ /app/strategy-service/strategy_templates/
COPY strategy-service/proto/ /app/strategy-service/proto/
COPY strategy-service/config.yaml /app/strategy-service/config.yaml
RUN mkdir -p /app/strategy-service/bin
COPY --from=go-builder /out/runtime-agent /app/strategy-service/bin/runtime-agent

# In the image we recreate the symlink layout the source tree uses so
# imports like `from utils.log import ...` resolve via PYTHONPATH the
# same way local dev does.
RUN ln -s /app/strategy-library /app/strategy-service/strategy-library \
    && cd /app/strategy-service \
    && uv sync --frozen --no-dev --no-install-package hushine-strategy-library

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV="/app/strategy-service/.venv" \
    PATH="/app/strategy-service/.venv/bin:$PATH" \
    PYTHONPATH="/app/strategy-service:/app/strategy-library"

WORKDIR /app/strategy-service

FROM runtime-base AS executor

ENV HUSHINE_RUNTIME_ROLE=executor

CMD ["./bin/runtime-agent", "--config", "config.yaml"]

FROM runtime-base AS executor-coverage

COPY --from=go-coverage-builder /out/runtime-agent /app/strategy-service/bin/runtime-agent
COPY strategy-service/.coveragerc /app/strategy-service/.coveragerc
RUN uv sync --frozen --no-dev --extra coverage \
    --no-install-package hushine-strategy-library

ENV HUSHINE_RUNTIME_ROLE=executor

CMD ["./bin/runtime-agent", "--config", "config.yaml"]

FROM executor AS default
