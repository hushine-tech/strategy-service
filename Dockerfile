# Phase D1 hosted strategy-runtime image.
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
# Hosted/default mode uses inbound gRPC plus hosted self-registration when
# the operator sets `RUNTIME_REGISTER_WITH_CONTROL_PANEL=1`. D3 self-hosted
# mode uses `RUNTIME_INGRESS_MODE=outbound` plus a mounted runtime credential
# and does not use RegisterRuntime. In outbound mode `run_grpc_server.py`
# strips account/order/Kafka/database endpoints from the loaded config and
# talks to the platform only through RuntimeChannel.

FROM python:3.13-slim AS runtime-base

# System deps for psycopg2-binary + grpcio compile-free wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps first so layer caches across code edits.
COPY strategy-service/requirements.txt /app/strategy-service/requirements.txt
COPY strategy-library/requirements.txt /app/strategy-library/requirements.txt
RUN pip install --no-cache-dir \
    -r /app/strategy-service/requirements.txt \
    -r /app/strategy-library/requirements.txt

# strategy-library is a sibling directory; the in-source `strategy-library/`
# symlink points to it. Copy the target so the image doesn't need symlink
# resolution at runtime.
COPY strategy-library/ /app/strategy-library/

# strategy-service code (everything except the dangling symlink).
COPY strategy-service/strategy_service/ /app/strategy-service/strategy_service/
COPY strategy-service/strategy_templates/ /app/strategy-service/strategy_templates/
COPY strategy-service/proto/ /app/strategy-service/proto/
COPY strategy-service/run_grpc_server.py /app/strategy-service/run_grpc_server.py
COPY strategy-service/run_http_server.py /app/strategy-service/run_http_server.py
COPY strategy-service/config.yaml /app/strategy-service/config.yaml

# In the image we recreate the symlink layout the source tree uses so
# imports like `from utils.log import ...` resolve via PYTHONPATH the
# same way local dev does.
RUN ln -s /app/strategy-library /app/strategy-service/strategy-library

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="/app/strategy-service:/app/strategy-library"

WORKDIR /app/strategy-service

EXPOSE 50053

FROM runtime-base AS executor

ENV HUSHINE_RUNTIME_ROLE=executor

# Executor runtimes use the normal strategy-service entrypoint. In D3
# deployments the control plane normally starts them in outbound
# RuntimeChannel mode.
CMD ["python", "run_grpc_server.py", "-config", "config.yaml"]

FROM python:3.12-slim AS debugger-base

# Keep the debugger image on Python 3.12 for PyCharm remote-debug stability,
# while executor stays on runtime-base/Python 3.13.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY strategy-service/requirements.txt /app/strategy-service/requirements.txt
COPY strategy-library/requirements.txt /app/strategy-library/requirements.txt
RUN pip install --no-cache-dir \
    -r /app/strategy-service/requirements.txt \
    -r /app/strategy-library/requirements.txt

COPY strategy-library/ /app/strategy-library/
COPY strategy-service/strategy_service/ /app/strategy-service/strategy_service/
COPY strategy-service/strategy_templates/ /app/strategy-service/strategy_templates/
COPY strategy-service/proto/ /app/strategy-service/proto/
COPY strategy-service/run_grpc_server.py /app/strategy-service/run_grpc_server.py
COPY strategy-service/run_http_server.py /app/strategy-service/run_http_server.py
COPY strategy-service/config.yaml /app/strategy-service/config.yaml

RUN ln -s /app/strategy-library /app/strategy-service/strategy-library

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="/app/strategy-service:/app/strategy-library"

WORKDIR /app/strategy-service

EXPOSE 50053

FROM debugger-base AS debugger

ARG PYDEVD_PYCHARM_VERSION=252.26199.168

ENV HUSHINE_RUNTIME_ROLE=debugger \
    HUSHINE_DEBUG_WORKSPACE=/workspace \
    HUSHINE_DEBUG_SOCKET=/tmp/hushine-debug.sock

RUN pip install --no-cache-dir debugpy "pydevd-pycharm~=${PYDEVD_PYCHARM_VERSION}" \
    && printf '#!/usr/bin/env sh\nexec python -m strategy_service.cli.hushine_debug "$@"\n' > /usr/local/bin/hushine-debug \
    && chmod +x /usr/local/bin/hushine-debug \
    && mkdir -p /workspace

EXPOSE 5678 5679

CMD ["python", "run_grpc_server.py", "-config", "config.yaml"]

FROM executor AS default
