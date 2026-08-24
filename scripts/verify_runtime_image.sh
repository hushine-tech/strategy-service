#!/usr/bin/env bash
# Verify the immutable facts and dependency closure of one final runtime image.

set -euo pipefail

usage() {
    echo "usage: $0 --image IMAGE --coverage true|false --profile NAME --version VERSION --digest SHA256 [--allow-dirty]" >&2
}

fail_usage() {
    echo "error: $1" >&2
    usage
    exit 2
}

IMAGE=""
COVERAGE=""
PROFILE=""
VERSION=""
DIGEST=""
ALLOW_DIRTY="false"
while (($#)); do
    case "$1" in
        --image|--coverage|--profile|--version|--digest)
            [[ $# -ge 2 ]] || fail_usage "$1 requires a value"
            case "$1" in
                --image) [[ -z "${IMAGE}" ]] || fail_usage "duplicate --image"; IMAGE="$2" ;;
                --coverage) [[ -z "${COVERAGE}" ]] || fail_usage "duplicate --coverage"; COVERAGE="$2" ;;
                --profile) [[ -z "${PROFILE}" ]] || fail_usage "duplicate --profile"; PROFILE="$2" ;;
                --version) [[ -z "${VERSION}" ]] || fail_usage "duplicate --version"; VERSION="$2" ;;
                --digest) [[ -z "${DIGEST}" ]] || fail_usage "duplicate --digest"; DIGEST="$2" ;;
            esac
            shift
            ;;
        --allow-dirty)
            [[ "${ALLOW_DIRTY}" == "false" ]] || fail_usage "duplicate --allow-dirty"
            ALLOW_DIRTY="true"
            ;;
        *) fail_usage "unknown argument: $1" ;;
    esac
    shift
done

[[ -n "${IMAGE}" ]] || fail_usage "--image is required"
[[ "${COVERAGE}" == "true" || "${COVERAGE}" == "false" ]] \
    || fail_usage "--coverage must be true or false"
[[ -n "${PROFILE}" ]] || fail_usage "--profile is required"
[[ -n "${VERSION}" ]] || fail_usage "--version is required"
[[ "${DIGEST}" =~ ^[0-9a-f]{64}$ ]] || fail_usage "--digest must be a lowercase SHA-256 value"

if ! INSPECT_JSON="$(docker image inspect "${IMAGE}")"; then
    echo "error: cannot inspect runtime image ${IMAGE}" >&2
    exit 1
fi

if ! HUSHINE_IMAGE_INSPECT_JSON="${INSPECT_JSON}" python3 - "${IMAGE}" "${COVERAGE}" "${PROFILE}" "${VERSION}" "${DIGEST}" "${ALLOW_DIRTY}" <<'PY'
import json
import os
import re
import sys

image, coverage, profile, version, digest, allow_dirty = sys.argv[1:]

def fail(fact):
    print(f"error: {image}: runtime image fact mismatch: {fact}", file=sys.stderr)
    raise SystemExit(1)

try:
    inspected = json.loads(os.environ.pop("HUSHINE_IMAGE_INSPECT_JSON"))
    config = inspected[0]["Config"]
    labels = config.get("Labels") or {}
    environment = {}
    for item in config.get("Env") or []:
        if "=" in item:
            key, value = item.split("=", 1)
            environment[key] = value
except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
    fail("image metadata")

label_names = {
    "profile": "org.hushine.runtime.profile",
    "profile_version": "org.hushine.runtime.profile.version",
    "digest": "org.hushine.runtime.contract.sha256",
    "service_commit": "org.hushine.runtime.strategy-service.commit",
    "library_commit": "org.hushine.runtime.strategy-library.commit",
    "golang_commit": "org.hushine.runtime.golang-lib.commit",
    "core_commit": "org.hushine.runtime.core-service.commit",
    "build_id": "org.hushine.runtime.image-build-id",
    "source_dirty": "org.hushine.runtime.source-dirty",
    "source_state": "org.hushine.runtime.source-state.sha256",
}
env_names = {
    "profile": "HUSHINE_RUNTIME_PROFILE_NAME",
    "profile_version": "HUSHINE_RUNTIME_PROFILE_VERSION",
    "digest": "HUSHINE_RUNTIME_CONTRACT_SHA256",
    "service_commit": "HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT",
    "library_commit": "HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT",
    "golang_commit": "HUSHINE_RUNTIME_GOLANG_LIB_COMMIT",
    "core_commit": "HUSHINE_RUNTIME_CORE_SERVICE_COMMIT",
    "build_id": "HUSHINE_RUNTIME_IMAGE_BUILD_ID",
    "source_dirty": "HUSHINE_RUNTIME_SOURCE_DIRTY",
    "source_state": "HUSHINE_RUNTIME_SOURCE_STATE_SHA256",
}
facts = {}
for name in label_names:
    label = labels.get(label_names[name])
    value = environment.get(env_names[name])
    if not isinstance(label, str) or not label or label != value:
        fail(name)
    facts[name] = label

for name, expected in (("profile", profile), ("profile_version", version), ("digest", digest)):
    if facts[name] != expected:
        fail(name)
for name in ("service_commit", "library_commit", "golang_commit", "core_commit"):
    if re.fullmatch(r"[0-9a-f]{40}", facts[name]) is None:
        fail(name)
if re.fullmatch(r"[0-9a-f]{64}", facts["source_state"]) is None:
    fail("source_state")
if facts["source_dirty"] not in {"true", "false"}:
    fail("source_dirty")
if facts["source_dirty"] == "true" and allow_dirty != "true":
    fail("source_dirty")

target = "executor-coverage" if coverage == "true" else "executor"
expected_prefix = "-".join(
    (
        facts["service_commit"][:12],
        facts["library_commit"][:12],
        facts["golang_commit"][:12],
        facts["core_commit"][:12],
        version,
        target,
    )
)
expected_build_id = expected_prefix
if facts["source_dirty"] == "true":
    expected_build_id += f"-dirty-{facts['source_state'][:12]}"
if facts["build_id"] != expected_build_id:
    fail("build_id")
PY
then
    exit 1
fi

PYTHON=/app/strategy-service/.venv/bin/python
docker run --rm --entrypoint uv "${IMAGE}" \
    pip check --python "${PYTHON}"
docker run --rm --entrypoint "${PYTHON}" "${IMAGE}" \
    /app/strategy-library/scripts/check_runtime_dependency_contract.py \
    --service-project /app/strategy-service/pyproject.toml \
    --service-lock /app/strategy-service/uv.lock \
    --installed-python "runtime=${PYTHON}" \
    --installed-python-version runtime=3.13 \
    --json
docker run --rm --entrypoint "${PYTHON}" "${IMAGE}" \
    -I -m hushine_strategy.runtime_dependencies verify-installed \
    --python-constraint 3.13 --json
docker run --rm --entrypoint "${PYTHON}" "${IMAGE}" \
    -I -c "from strategy_service import session_worker_entry; from strategy_service.gen import strategy_service_pb2, runtime_worker_pb2, control_panel_service_pb2"
docker run --rm --entrypoint "${PYTHON}" "${IMAGE}" \
    /app/strategy-service/scripts/runtime_dependency_worker_smoke.py \
    --strategy-body /app/strategy-service/scripts/fixtures/runtime_dependency_strategy_body.py \
    --expected-profile "${PROFILE}" \
    --expected-version "${VERSION}" \
    --expected-digest "${DIGEST}" \
    --coverage "${COVERAGE}" \
    --check-only

echo "Verified runtime image ${IMAGE}."
