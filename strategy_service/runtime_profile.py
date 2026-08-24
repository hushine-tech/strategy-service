from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
import os
import re

from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile


_BUILD_FACT_KEYS = (
    "HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT",
    "HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT",
    "HUSHINE_RUNTIME_GOLANG_LIB_COMMIT",
    "HUSHINE_RUNTIME_CORE_SERVICE_COMMIT",
    "HUSHINE_RUNTIME_IMAGE_BUILD_ID",
)
_CONFIGURATION_ERROR = "invalid runtime build identity configuration"
_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
_SEMVER_PATTERN = (
    r"(?:0|[1-9][0-9]*)\."
    r"(?:0|[1-9][0-9]*)\."
    r"(?:0|[1-9][0-9]*)"
    r"(?:-(?:"
    r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*"
    r"))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)
_IMAGE_BUILD_ID_PATTERN = re.compile(
    rf"(?P<service_commit>[0-9a-f]{{12}})-"
    rf"(?P<library_commit>[0-9a-f]{{12}})-"
    rf"(?P<golang_lib_commit>[0-9a-f]{{12}})-"
    rf"(?P<core_service_commit>[0-9a-f]{{12}})-"
    rf"(?P<profile_version>{_SEMVER_PATTERN})-"
    rf"(?P<target>executor(?:-coverage)?)"
    rf"(?:-dirty-[0-9a-f]{{12}})?"
)


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    version: str
    contract_sha256: str
    hosted_python: str
    allowed_third_party_modules: tuple[str, ...]
    strategy_service_commit: str
    strategy_library_commit: str
    golang_lib_commit: str
    core_service_commit: str
    image_build_id: str


def _invalid_configuration() -> RuntimeError:
    return RuntimeError(_CONFIGURATION_ERROR)


def _build_facts_from_environment(
    environment: Mapping[str, str],
    *,
    profile_version: str,
) -> tuple[str, str, str, str, str]:
    present = tuple(key in environment for key in _BUILD_FACT_KEYS)
    if not any(present):
        return ("local-dev", "local-dev", "local-dev", "local-dev", "local-dev")
    if not all(present):
        raise _invalid_configuration()

    values = tuple(environment[key] for key in _BUILD_FACT_KEYS)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise _invalid_configuration()
    service_commit, library_commit, golang_lib_commit, core_service_commit, image_build_id = values
    if (
        _COMMIT_PATTERN.fullmatch(service_commit) is None
        or _COMMIT_PATTERN.fullmatch(library_commit) is None
        or _COMMIT_PATTERN.fullmatch(golang_lib_commit) is None
        or _COMMIT_PATTERN.fullmatch(core_service_commit) is None
    ):
        raise _invalid_configuration()
    try:
        image_build_id_bytes = image_build_id.encode("ascii")
    except UnicodeEncodeError:
        raise _invalid_configuration() from None
    build_match = _IMAGE_BUILD_ID_PATTERN.fullmatch(image_build_id)
    if len(image_build_id_bytes) > 96 or build_match is None:
        raise _invalid_configuration()
    if (
        build_match.group("service_commit") != service_commit[:12]
        or build_match.group("library_commit") != library_commit[:12]
        or build_match.group("golang_lib_commit") != golang_lib_commit[:12]
        or build_match.group("core_service_commit") != core_service_commit[:12]
        or build_match.group("profile_version") != profile_version
    ):
        raise _invalid_configuration()
    return service_commit, library_commit, golang_lib_commit, core_service_commit, image_build_id


def _runtime_profile_from_environment(
    environment: Mapping[str, str],
) -> RuntimeProfile:
    manifest = load_runtime_dependency_profile()
    service_commit, library_commit, golang_lib_commit, core_service_commit, image_build_id = (
        _build_facts_from_environment(
            environment,
            profile_version=manifest.profile_version,
        )
    )
    return RuntimeProfile(
        name=manifest.profile_name,
        version=manifest.profile_version,
        contract_sha256=manifest.contract_sha256,
        hosted_python=manifest.hosted_python,
        allowed_third_party_modules=manifest.public_import_roots,
        strategy_service_commit=service_commit,
        strategy_library_commit=library_commit,
        golang_lib_commit=golang_lib_commit,
        core_service_commit=core_service_commit,
        image_build_id=image_build_id,
    )


@lru_cache(maxsize=1)
def current_runtime_profile() -> RuntimeProfile:
    return _runtime_profile_from_environment(os.environ)
