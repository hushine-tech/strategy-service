#!/usr/bin/env bash
# Run final image verification, a real one-shot worker, and runtime-agent help.

set -euo pipefail

usage() {
    echo "usage: $0 --image IMAGE --coverage true|false --profile NAME --version VERSION --digest SHA256 [--allow-dirty]" >&2
}

fail_usage() {
    echo "error: $1" >&2
    usage
    exit 2
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
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

VERIFY_ARGS=(
    --image "${IMAGE}"
    --coverage "${COVERAGE}"
    --profile "${PROFILE}"
    --version "${VERSION}"
    --digest "${DIGEST}"
)
[[ "${ALLOW_DIRTY}" == "false" ]] || VERIFY_ARGS+=(--allow-dirty)
"${SCRIPT_DIR}/verify_runtime_image.sh" "${VERIFY_ARGS[@]}"

docker run --rm --entrypoint /app/strategy-service/.venv/bin/python "${IMAGE}" \
    /app/strategy-service/scripts/runtime_dependency_worker_smoke.py \
    --strategy-body /app/strategy-service/scripts/fixtures/runtime_dependency_strategy_body.py \
    --expected-profile "${PROFILE}" \
    --expected-version "${VERSION}" \
    --expected-digest "${DIGEST}" \
    --coverage "${COVERAGE}"
docker run --rm --entrypoint ./bin/runtime-agent "${IMAGE}" --help

echo "All runtime image smoke checks passed for ${IMAGE}."
