#!/usr/bin/env bash
# Build normal and/or coverage runtime images from a sealed Git-derived context.

set -euo pipefail

usage() {
    echo "usage: $0 [--coverage|--all] [--no-cache] [--verify] [--allow-dirty] VERSION" >&2
}

fail_usage() {
    echo "error: $1" >&2
    usage
    exit 2
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_DIR="$(cd "${SERVICE_DIR}/.." && pwd)"
LIBRARY_DIR="${WORKSPACE_DIR}/strategy-library"
GOLANG_LIB_DIR="${WORKSPACE_DIR}/golang-lib"
IMAGE_PREFIX="${IMAGE_PREFIX:-hushine/strategy-runtime}"

MODE="normal"
MODE_SELECTED="false"
NO_CACHE="false"
VERIFY="false"
ALLOW_DIRTY="false"
VERSION=""

while (($#)); do
    case "$1" in
        --coverage|--all)
            if [[ "${MODE_SELECTED}" == "true" ]]; then
                fail_usage "--coverage and --all are mutually exclusive"
            fi
            MODE_SELECTED="true"
            [[ "$1" == "--coverage" ]] && MODE="coverage" || MODE="all"
            ;;
        --no-cache)
            [[ "${NO_CACHE}" == "false" ]] || fail_usage "duplicate --no-cache"
            NO_CACHE="true"
            ;;
        --verify)
            [[ "${VERIFY}" == "false" ]] || fail_usage "duplicate --verify"
            VERIFY="true"
            ;;
        --allow-dirty)
            [[ "${ALLOW_DIRTY}" == "false" ]] || fail_usage "duplicate --allow-dirty"
            ALLOW_DIRTY="true"
            ;;
        --*)
            fail_usage "unknown option: $1"
            ;;
        *)
            [[ -z "${VERSION}" ]] || fail_usage "exactly one VERSION is required"
            VERSION="$1"
            ;;
    esac
    shift
done

[[ -n "${VERSION}" ]] || fail_usage "VERSION is required"
[[ "${VERSION}" != "coverage" ]] || fail_usage "version 'coverage' is reserved"
[[ "${VERSION}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] \
    || fail_usage "VERSION is not a valid Docker tag component"

for repository in "${SERVICE_DIR}" "${LIBRARY_DIR}" "${GOLANG_LIB_DIR}"; do
    [[ -d "${repository}/.git" || -f "${repository}/.git" ]] \
        || fail_usage "missing Git repository: ${repository}"
done

PROFILE_VALUES="$(python3 - "${LIBRARY_DIR}/hushine_strategy/runtime_dependencies.toml" <<'PY'
import hashlib
from pathlib import Path
import sys
import tomllib

path = Path(sys.argv[1])
raw = path.read_bytes()
profile = tomllib.loads(raw.decode("utf-8"))
print(profile["profile_name"])
print(profile["profile_version"])
print(hashlib.sha256(raw).hexdigest())
print(profile["hosted_python"])
print(",".join(sorted(
    item["import_root"] for item in profile["dependencies"] if item["public"]
)))
PY
)"
PROFILE_NAME="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '1p')"
PROFILE_VERSION="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '2p')"
CONTRACT_SHA256="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '3p')"
HOSTED_PYTHON="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '4p')"
PUBLIC_IMPORT_ROOTS="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '5p')"
[[ -n "${PROFILE_NAME}" && -n "${PROFILE_VERSION}" && ${#CONTRACT_SHA256} -eq 64 && "${HOSTED_PYTHON}" == "3.13" && -n "${PUBLIC_IMPORT_ROOTS}" ]] \
    || fail_usage "invalid runtime dependency profile"

CONTEXT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hushine-runtime-context-XXXXXX")"
chmod 700 "${CONTEXT_DIR}"
cleanup() {
    rm -rf -- "${CONTEXT_DIR}"
}
trap cleanup EXIT

CONTEXT_ARGUMENTS=(
    --output "${CONTEXT_DIR}"
    --service-repository "${SERVICE_DIR}"
    --library-repository "${LIBRARY_DIR}"
    --golang-lib-repository "${GOLANG_LIB_DIR}"
    --profile-digest "${CONTRACT_SHA256}"
)
if [[ "${ALLOW_DIRTY}" == "true" ]]; then
    CONTEXT_ARGUMENTS+=(--allow-dirty)
fi
set +e
CONTEXT_JSON="$(python3 "${SCRIPT_DIR}/prepare_runtime_build_context.py" "${CONTEXT_ARGUMENTS[@]}")"
CONTEXT_STATUS=$?
set -e
[[ ${CONTEXT_STATUS} -eq 0 ]] || exit 2

json_value() {
    python3 -c 'import json,sys; value=json.loads(sys.argv[1]); print(value[sys.argv[2]])' \
        "${CONTEXT_JSON}" "$1"
}
json_commit() {
    python3 -c 'import json,sys; value=json.loads(sys.argv[1]); print(value["commits"][sys.argv[2]])' \
        "${CONTEXT_JSON}" "$1"
}

SERVICE_COMMIT="$(json_commit strategy-service)"
LIBRARY_COMMIT="$(json_commit strategy-library)"
GOLANG_LIB_COMMIT="$(json_commit golang-lib)"
SOURCE_DIRTY="$(json_value source_dirty)"
SOURCE_STATE_SHA256="$(json_value source_state_sha256)"
[[ "${SOURCE_DIRTY}" == "True" ]] && SOURCE_DIRTY="true" || SOURCE_DIRTY="false"

SERVICE_COMMIT_SHORT="${SERVICE_COMMIT:0:12}"
LIBRARY_COMMIT_SHORT="${LIBRARY_COMMIT:0:12}"
GOLANG_LIB_COMMIT_SHORT="${GOLANG_LIB_COMMIT:0:12}"

validate_build_id() {
    local value="$1"
    [[ -n "${value}" && ${#value} -le 96 ]] || fail_usage "invalid IMAGE_BUILD_ID"
    [[ ! "${value}" =~ [[:space:][:cntrl:]] ]] || fail_usage "invalid IMAGE_BUILD_ID"
}

default_build_id() {
    local target="$1"
    local value="${SERVICE_COMMIT_SHORT}-${LIBRARY_COMMIT_SHORT}-${GOLANG_LIB_COMMIT_SHORT}-${PROFILE_VERSION}-${target}"
    if [[ "${SOURCE_DIRTY}" == "true" ]]; then
        value="${value}-dirty-${SOURCE_STATE_SHA256:0:12}"
    fi
    printf '%s' "${value}"
}

build_id_for() {
    local target="$1"
    local override=""
    local override_set="false"
    if [[ "${MODE}" != "all" && -n "${IMAGE_BUILD_ID+x}" ]]; then
        override="${IMAGE_BUILD_ID}"
        override_set="true"
    elif [[ "${target}" == "executor" && -n "${EXECUTOR_IMAGE_BUILD_ID+x}" ]]; then
        override="${EXECUTOR_IMAGE_BUILD_ID}"
        override_set="true"
    elif [[ "${target}" == "executor-coverage" && -n "${EXECUTOR_COVERAGE_IMAGE_BUILD_ID+x}" ]]; then
        override="${EXECUTOR_COVERAGE_IMAGE_BUILD_ID}"
        override_set="true"
    fi
    if [[ "${override_set}" == "true" ]]; then
        validate_build_id "${override}"
        [[ "${override}" == "$(default_build_id "${target}")" ]] \
            || fail_usage "explicit IMAGE_BUILD_ID does not match sealed target identity"
        printf '%s' "${override}"
    else
        default_build_id "${target}"
    fi
}

if [[ "${MODE}" == "all" && -n "${IMAGE_BUILD_ID+x}" ]]; then
    fail_usage "IMAGE_BUILD_ID cannot be shared by --all targets"
fi
if [[ "${MODE}" != "all" && -n "${IMAGE_BUILD_ID+x}" ]]; then
    if [[ "${MODE}" == "normal" && -n "${EXECUTOR_IMAGE_BUILD_ID+x}" ]]; then
        fail_usage "IMAGE_BUILD_ID and EXECUTOR_IMAGE_BUILD_ID are mutually exclusive"
    fi
    if [[ "${MODE}" == "coverage" && -n "${EXECUTOR_COVERAGE_IMAGE_BUILD_ID+x}" ]]; then
        fail_usage "IMAGE_BUILD_ID and EXECUTOR_COVERAGE_IMAGE_BUILD_ID are mutually exclusive"
    fi
fi

NORMAL_IMAGE_BUILD_ID=""
COVERAGE_IMAGE_BUILD_ID=""
if [[ "${MODE}" == "normal" || "${MODE}" == "all" ]]; then
    NORMAL_IMAGE_BUILD_ID="$(build_id_for executor)"
    validate_build_id "${NORMAL_IMAGE_BUILD_ID}"
fi
if [[ "${MODE}" == "coverage" || "${MODE}" == "all" ]]; then
    COVERAGE_IMAGE_BUILD_ID="$(build_id_for executor-coverage)"
    validate_build_id "${COVERAGE_IMAGE_BUILD_ID}"
fi
if [[ "${MODE}" == "all" ]]; then
    [[ "${NORMAL_IMAGE_BUILD_ID}" != "${COVERAGE_IMAGE_BUILD_ID}" ]] \
        || fail_usage "normal and coverage IMAGE_BUILD_ID values must differ"
fi

common_build_args() {
    local build_id="$1"
    printf '%s\n' \
        "RUNTIME_PROFILE_NAME=${PROFILE_NAME}" \
        "RUNTIME_PROFILE_VERSION=${PROFILE_VERSION}" \
        "RUNTIME_CONTRACT_SHA256=${CONTRACT_SHA256}" \
        "RUNTIME_HOSTED_PYTHON=${HOSTED_PYTHON}" \
        "RUNTIME_PUBLIC_IMPORT_ROOTS=${PUBLIC_IMPORT_ROOTS}" \
        "RUNTIME_STRATEGY_SERVICE_COMMIT=${SERVICE_COMMIT}" \
        "RUNTIME_STRATEGY_LIBRARY_COMMIT=${LIBRARY_COMMIT}" \
        "RUNTIME_GOLANG_LIB_COMMIT=${GOLANG_LIB_COMMIT}" \
        "RUNTIME_IMAGE_BUILD_ID=${build_id}" \
        "RUNTIME_SOURCE_DIRTY=${SOURCE_DIRTY}" \
        "RUNTIME_SOURCE_STATE_SHA256=${SOURCE_STATE_SHA256}"
}

build_target() {
    local target="$1"
    local image="$2"
    local build_id="$3"
    shift 3
    local tags=("$@")
    local command=(docker build)
    [[ "${NO_CACHE}" == "false" ]] || command+=(--no-cache)
    command+=(--target "${target}" -f "${CONTEXT_DIR}/strategy-service/Dockerfile")
    local value
    while IFS= read -r value; do
        command+=(--build-arg "${value}")
    done < <(common_build_args "${build_id}")
    for value in "${tags[@]}"; do
        command+=(-t "${value}")
    done
    command+=("${CONTEXT_DIR}")
    echo "Building ${image} (${build_id})"
    "${command[@]}"
    if [[ "${VERIFY}" == "true" ]]; then
        local verify_args=(
            --image "${image}"
            --coverage false
            --profile "${PROFILE_NAME}"
            --version "${PROFILE_VERSION}"
            --digest "${CONTRACT_SHA256}"
        )
        [[ "${target}" == "executor" ]] || verify_args[3]="true"
        [[ "${ALLOW_DIRTY}" == "false" ]] || verify_args+=(--allow-dirty)
        "${SCRIPT_DIR}/verify_runtime_image.sh" "${verify_args[@]}"
    fi
}

NORMAL_IMAGE="${IMAGE_PREFIX}:executor-${VERSION}"
COVERAGE_IMAGE="${IMAGE_PREFIX}:executor-coverage-${VERSION}"
if [[ "${MODE}" == "normal" || "${MODE}" == "all" ]]; then
    build_target \
        executor \
        "${NORMAL_IMAGE}" \
        "${NORMAL_IMAGE_BUILD_ID}" \
        "${NORMAL_IMAGE}" \
        "${IMAGE_PREFIX}:executor" \
        "${IMAGE_PREFIX}:dev" \
        "${IMAGE_PREFIX}:${VERSION}"
fi
if [[ "${MODE}" == "coverage" || "${MODE}" == "all" ]]; then
    build_target \
        executor-coverage \
        "${COVERAGE_IMAGE}" \
        "${COVERAGE_IMAGE_BUILD_ID}" \
        "${COVERAGE_IMAGE}"
fi

echo "Runtime image build completed for ${VERSION}."
