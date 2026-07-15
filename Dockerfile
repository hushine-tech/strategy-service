# Hosted/self-hosted executor runtime. The build context is a sealed temporary
# directory containing only Git-derived strategy-service, strategy-library,
# and golang-lib inputs (created by scripts/prepare_runtime_build_context.py).

FROM ghcr.io/astral-sh/uv:0.11.16@sha256:440fd6477af86a2f1b38080c539f1672cd22acb1b1a47e321dba5158ab08864d AS uv-bin

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

ARG RUNTIME_PROFILE_NAME
ARG RUNTIME_PROFILE_VERSION
ARG RUNTIME_CONTRACT_SHA256
ARG RUNTIME_HOSTED_PYTHON
ARG RUNTIME_PUBLIC_IMPORT_ROOTS
ARG RUNTIME_STRATEGY_SERVICE_COMMIT
ARG RUNTIME_STRATEGY_LIBRARY_COMMIT
ARG RUNTIME_GOLANG_LIB_COMMIT
ARG RUNTIME_IMAGE_BUILD_ID
ARG RUNTIME_SOURCE_DIRTY
ARG RUNTIME_SOURCE_STATE_SHA256

LABEL org.hushine.runtime.profile=${RUNTIME_PROFILE_NAME} \
      org.hushine.runtime.profile.version=${RUNTIME_PROFILE_VERSION} \
      org.hushine.runtime.contract.sha256=${RUNTIME_CONTRACT_SHA256} \
      org.hushine.runtime.strategy-service.commit=${RUNTIME_STRATEGY_SERVICE_COMMIT} \
      org.hushine.runtime.strategy-library.commit=${RUNTIME_STRATEGY_LIBRARY_COMMIT} \
      org.hushine.runtime.golang-lib.commit=${RUNTIME_GOLANG_LIB_COMMIT} \
      org.hushine.runtime.image-build-id=${RUNTIME_IMAGE_BUILD_ID} \
      org.hushine.runtime.source-dirty=${RUNTIME_SOURCE_DIRTY} \
      org.hushine.runtime.source-state.sha256=${RUNTIME_SOURCE_STATE_SHA256}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/app/strategy-service/.venv \
    PATH=/app/strategy-service/.venv/bin:/usr/local/bin:$PATH \
    HUSHINE_RUNTIME_PROFILE_NAME=${RUNTIME_PROFILE_NAME} \
    HUSHINE_RUNTIME_PROFILE_VERSION=${RUNTIME_PROFILE_VERSION} \
    HUSHINE_RUNTIME_CONTRACT_SHA256=${RUNTIME_CONTRACT_SHA256} \
    HUSHINE_RUNTIME_HOSTED_PYTHON=${RUNTIME_HOSTED_PYTHON} \
    HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS=${RUNTIME_PUBLIC_IMPORT_ROOTS} \
    HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT=${RUNTIME_STRATEGY_SERVICE_COMMIT} \
    HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT=${RUNTIME_STRATEGY_LIBRARY_COMMIT} \
    HUSHINE_RUNTIME_GOLANG_LIB_COMMIT=${RUNTIME_GOLANG_LIB_COMMIT} \
    HUSHINE_RUNTIME_IMAGE_BUILD_ID=${RUNTIME_IMAGE_BUILD_ID} \
    HUSHINE_RUNTIME_SOURCE_DIRTY=${RUNTIME_SOURCE_DIRTY} \
    HUSHINE_RUNTIME_SOURCE_STATE_SHA256=${RUNTIME_SOURCE_STATE_SHA256}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv-bin /uv /uvx /usr/local/bin/

WORKDIR /app

# Both local projects must exist before the frozen non-editable sync so uv can
# install their exact locked source distributions rather than source-shadowing.
COPY strategy-library/pyproject.toml strategy-library/README.md /app/strategy-library/
COPY strategy-library/algo/ /app/strategy-library/algo/
COPY strategy-library/hushine_runtime_import_probe/ /app/strategy-library/hushine_runtime_import_probe/
COPY strategy-library/hushine_strategy/ /app/strategy-library/hushine_strategy/
COPY strategy-library/market_data/ /app/strategy-library/market_data/
COPY strategy-library/strategy_service/ /app/strategy-library/strategy_service/
COPY strategy-library/utils/ /app/strategy-library/utils/
COPY strategy-library/scripts/check_runtime_dependency_contract.py /app/strategy-library/scripts/check_runtime_dependency_contract.py
COPY strategy-service/pyproject.toml /app/strategy-service/pyproject.toml
COPY strategy-service/uv.lock /app/strategy-service/uv.lock
COPY strategy-service/strategy_service/ /app/strategy-service/strategy_service/
COPY strategy-service/strategy_templates/ /app/strategy-service/strategy_templates/
COPY strategy-service/proto/ /app/strategy-service/proto/
COPY strategy-service/config.yaml /app/strategy-service/config.yaml
COPY strategy-service/scripts/runtime_dependency_worker_smoke.py /app/strategy-service/scripts/runtime_dependency_worker_smoke.py
COPY strategy-service/scripts/fixtures/runtime_dependency_strategy_body.py /app/strategy-service/scripts/fixtures/runtime_dependency_strategy_body.py
RUN mkdir -p /app/strategy-service/bin
COPY --from=go-builder /out/runtime-agent /app/strategy-service/bin/runtime-agent

WORKDIR /app/strategy-service
RUN uv sync --frozen --no-dev --no-editable \
    && uv pip check --python /app/strategy-service/.venv/bin/python
RUN /app/strategy-service/.venv/bin/python \
      /app/strategy-library/scripts/check_runtime_dependency_contract.py \
      --service-project /app/strategy-service/pyproject.toml \
      --service-lock /app/strategy-service/uv.lock \
      --installed-python runtime=/app/strategy-service/.venv/bin/python \
      --installed-python-version runtime=3.13 \
      --json \
    && /app/strategy-service/.venv/bin/python -I \
      -m hushine_strategy.runtime_dependencies verify-installed \
      --python-constraint 3.13 --json \
    && /app/strategy-service/.venv/bin/python -I -c \
      "from strategy_service import session_worker_entry; from strategy_service.gen import strategy_service_pb2, runtime_worker_pb2, control_panel_service_pb2" \
    && /app/strategy-service/.venv/bin/python \
      /app/strategy-service/scripts/runtime_dependency_worker_smoke.py \
      --strategy-body /app/strategy-service/scripts/fixtures/runtime_dependency_strategy_body.py \
      --expected-profile "${HUSHINE_RUNTIME_PROFILE_NAME}" \
      --expected-version "${HUSHINE_RUNTIME_PROFILE_VERSION}" \
      --expected-digest "${HUSHINE_RUNTIME_CONTRACT_SHA256}" \
      --coverage false --check-only

FROM runtime-base AS executor
ENV HUSHINE_RUNTIME_ROLE=executor
CMD ["./bin/runtime-agent", "--config", "config.yaml"]

FROM runtime-base AS executor-coverage
COPY --from=go-coverage-builder /out/runtime-agent /app/strategy-service/bin/runtime-agent
COPY strategy-service/.coveragerc /app/strategy-service/.coveragerc
RUN uv sync --frozen --no-dev --extra coverage --no-editable \
    && uv pip check --python /app/strategy-service/.venv/bin/python
RUN /app/strategy-service/.venv/bin/python \
      /app/strategy-library/scripts/check_runtime_dependency_contract.py \
      --service-project /app/strategy-service/pyproject.toml \
      --service-lock /app/strategy-service/uv.lock \
      --installed-python runtime=/app/strategy-service/.venv/bin/python \
      --installed-python-version runtime=3.13 \
      --json \
    && /app/strategy-service/.venv/bin/python -I \
      -m hushine_strategy.runtime_dependencies verify-installed \
      --python-constraint 3.13 --json \
    && /app/strategy-service/.venv/bin/python -I -c \
      "from strategy_service import session_worker_entry; from strategy_service.gen import strategy_service_pb2, runtime_worker_pb2, control_panel_service_pb2" \
    && /app/strategy-service/.venv/bin/python \
      /app/strategy-service/scripts/runtime_dependency_worker_smoke.py \
      --strategy-body /app/strategy-service/scripts/fixtures/runtime_dependency_strategy_body.py \
      --expected-profile "${HUSHINE_RUNTIME_PROFILE_NAME}" \
      --expected-version "${HUSHINE_RUNTIME_PROFILE_VERSION}" \
      --expected-digest "${HUSHINE_RUNTIME_CONTRACT_SHA256}" \
      --coverage true --check-only
ENV HUSHINE_RUNTIME_ROLE=executor
CMD ["./bin/runtime-agent", "--config", "config.yaml"]

FROM executor AS default
