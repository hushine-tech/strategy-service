import itertools
from dataclasses import replace

import pytest

from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile
import strategy_service.runtime_profile as runtime_profile_module
from strategy_service.runtime_profile import (
    _runtime_profile_from_environment,
    current_runtime_profile,
)


BUILD_FACT_KEYS = (
    "HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT",
    "HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT",
    "HUSHINE_RUNTIME_IMAGE_BUILD_ID",
)
VALID_BUILD_FACTS = {
    BUILD_FACT_KEYS[0]: "a" * 40,
    BUILD_FACT_KEYS[1]: "b" * 40,
    BUILD_FACT_KEYS[2]: f"{'a' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor",
}
SAFE_CONFIGURATION_ERROR = "invalid runtime build identity configuration"


@pytest.fixture(autouse=True)
def _reset_runtime_profile_cache():
    current_runtime_profile.cache_clear()
    yield
    current_runtime_profile.cache_clear()


def test_all_missing_build_facts_use_one_local_dev_identity():
    manifest = load_runtime_dependency_profile()
    profile = _runtime_profile_from_environment({})

    assert profile.name == manifest.profile_name
    assert profile.version == manifest.profile_version
    assert profile.contract_sha256 == manifest.contract_sha256
    assert profile.hosted_python == manifest.hosted_python
    assert profile.allowed_third_party_modules == manifest.public_import_roots
    assert profile.strategy_service_commit == "local-dev"
    assert profile.strategy_library_commit == "local-dev"
    assert profile.image_build_id == "local-dev"


@pytest.mark.parametrize(
    "image_build_id",
    [
        f"{'a' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor",
        f"{'a' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor-coverage",
        f"{'a' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor-dirty-{'d' * 12}",
    ],
)
def test_all_present_build_facts_are_preserved_exactly(image_build_id):
    environment = {**VALID_BUILD_FACTS, BUILD_FACT_KEYS[2]: image_build_id}
    profile = _runtime_profile_from_environment(environment)
    assert profile.strategy_service_commit == environment[BUILD_FACT_KEYS[0]]
    assert profile.strategy_library_commit == environment[BUILD_FACT_KEYS[1]]
    assert profile.image_build_id == image_build_id


PARTIAL_BUILD_FACT_KEY_SETS = [
    present
    for count in (1, 2)
    for present in itertools.combinations(BUILD_FACT_KEYS, count)
]


@pytest.mark.parametrize("present_keys", PARTIAL_BUILD_FACT_KEY_SETS)
def test_partial_build_fact_combinations_fail_closed(present_keys):
    environment = {key: VALID_BUILD_FACTS[key] for key in present_keys}
    with pytest.raises(RuntimeError, match=f"^{SAFE_CONFIGURATION_ERROR}$"):
        _runtime_profile_from_environment(environment)


@pytest.mark.parametrize("key", BUILD_FACT_KEYS)
@pytest.mark.parametrize("blank", ["", " ", "\t", "\n"])
def test_blank_build_facts_fail_closed_without_echoing_value(key, blank):
    environment = {**VALID_BUILD_FACTS, key: blank}
    with pytest.raises(RuntimeError) as exc_info:
        _runtime_profile_from_environment(environment)
    assert str(exc_info.value) == SAFE_CONFIGURATION_ERROR


@pytest.mark.parametrize("key", BUILD_FACT_KEYS[:2])
@pytest.mark.parametrize(
    "poisoned",
    [
        "a" * 39,
        "A" * 40,
        "a" * 39 + "/",
        "a" * 39 + "\n",
        "ghp_" + "a" * 36,
        "é" * 40,
    ],
)
def test_malformed_commit_facts_fail_with_constant_safe_error(key, poisoned):
    environment = {**VALID_BUILD_FACTS, key: poisoned}
    with pytest.raises(RuntimeError) as exc_info:
        _runtime_profile_from_environment(environment)
    assert str(exc_info.value) == SAFE_CONFIGURATION_ERROR
    assert poisoned not in str(exc_info.value)


@pytest.mark.parametrize(
    "poisoned",
    [
        "build-1",
        "/tmp/runtime-image",
        "token=super-secret",
        "a\nexecutor",
        "a\x00executor",
        "é" * 40,
        f"{'A' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor",
        f"{'a' * 12}-{'b' * 12}-{'c' * 12}-01.0.0-executor",
        f"{'a' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-worker",
        f"{'a' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor-dirty-short",
    ],
)
def test_malformed_image_build_ids_fail_with_constant_safe_error(poisoned):
    environment = {**VALID_BUILD_FACTS, BUILD_FACT_KEYS[2]: poisoned}
    with pytest.raises(RuntimeError) as exc_info:
        _runtime_profile_from_environment(environment)
    assert str(exc_info.value) == SAFE_CONFIGURATION_ERROR
    assert poisoned not in str(exc_info.value)


@pytest.mark.parametrize(
    "image_build_id",
    [
        f"{'d' * 12}-{'b' * 12}-{'c' * 12}-1.0.0-executor",
        f"{'a' * 12}-{'d' * 12}-{'c' * 12}-1.0.0-executor",
    ],
)
def test_image_build_id_short_commits_must_match_full_commit_prefixes(
    image_build_id,
):
    environment = {**VALID_BUILD_FACTS, BUILD_FACT_KEYS[2]: image_build_id}
    with pytest.raises(RuntimeError, match=f"^{SAFE_CONFIGURATION_ERROR}$"):
        _runtime_profile_from_environment(environment)


def test_image_build_id_semver_must_match_loaded_profile_version():
    image_build_id = f"{'a' * 12}-{'b' * 12}-{'c' * 12}-2.0.0-executor"
    with pytest.raises(RuntimeError, match=f"^{SAFE_CONFIGURATION_ERROR}$"):
        _runtime_profile_from_environment(
            {**VALID_BUILD_FACTS, BUILD_FACT_KEYS[2]: image_build_id}
        )


def test_image_build_id_accepts_96_ascii_bytes_and_rejects_97(monkeypatch):
    prefix = f"{'a' * 12}-{'b' * 12}-{'c' * 12}-"
    version_at_limit = "1.0.0+" + ("d" * 42)
    oversized_version = "1.0.0+" + ("d" * 43)
    exactly_96 = prefix + version_at_limit + "-executor"
    oversized = prefix + oversized_version + "-executor"
    assert len(exactly_96.encode("ascii")) == 96
    assert len(oversized.encode("ascii")) == 97

    manifest = load_runtime_dependency_profile()
    monkeypatch.setattr(
        runtime_profile_module,
        "load_runtime_dependency_profile",
        lambda: replace(manifest, profile_version=version_at_limit),
    )
    profile = _runtime_profile_from_environment(
        {**VALID_BUILD_FACTS, BUILD_FACT_KEYS[2]: exactly_96}
    )
    assert profile.image_build_id == exactly_96
    monkeypatch.setattr(
        runtime_profile_module,
        "load_runtime_dependency_profile",
        lambda: replace(manifest, profile_version=oversized_version),
    )
    with pytest.raises(RuntimeError, match=f"^{SAFE_CONFIGURATION_ERROR}$"):
        _runtime_profile_from_environment(
            {**VALID_BUILD_FACTS, BUILD_FACT_KEYS[2]: oversized}
        )


def test_current_runtime_profile_is_loaded_once_per_process(monkeypatch):
    for key in BUILD_FACT_KEYS:
        monkeypatch.delenv(key, raising=False)
    first = current_runtime_profile()
    for key, value in VALID_BUILD_FACTS.items():
        monkeypatch.setenv(key, value)

    assert current_runtime_profile() is first
    current_runtime_profile.cache_clear()
    rebuilt = current_runtime_profile()
    assert rebuilt.strategy_service_commit == VALID_BUILD_FACTS[BUILD_FACT_KEYS[0]]
