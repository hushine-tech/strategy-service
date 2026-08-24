from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys

from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile

from scripts.runtime_dependency_worker_smoke import representative_strategy_source


SERVICE_DIR = Path(__file__).resolve().parents[1]
DOCKERFILE = SERVICE_DIR / "Dockerfile"
FIXTURE = (
    SERVICE_DIR
    / "scripts"
    / "fixtures"
    / "runtime_dependency_strategy_body.py"
)


def _dockerfile_stage(text: str, name: str) -> str:
    marker = f" AS {name}"
    lines = text.splitlines()
    start = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("FROM ") and line.endswith(marker)
    )
    end = next(
        (
            index
            for index, line in enumerate(lines[start + 1 :], start=start + 1)
            if line.startswith("FROM ")
        ),
        len(lines),
    )
    return "\n".join(lines[start:end])


def test_strategy_runtime_image_has_only_executor_targets():
    content = DOCKERFILE.read_text(encoding="utf-8")

    assert "FROM python:3.13-slim AS runtime-base" in content
    assert "FROM runtime-base AS executor" in content
    assert "FROM runtime-base AS executor-coverage" in content
    assert "FROM executor AS default" in content
    assert "AS debugger" not in content
    assert "debugpy" not in content
    assert "pydevd-pycharm" not in content


def test_go_proxy_override_is_confined_to_builder_stages():
    text = DOCKERFILE.read_text(encoding="utf-8")
    go_builder = _dockerfile_stage(text, "go-builder-base")
    runtime_base = _dockerfile_stage(text, "runtime-base")

    assert (
        "ARG RUNTIME_GO_PROXY=https://proxy.golang.org,direct" in go_builder
    )
    assert "ENV GOPROXY=${RUNTIME_GO_PROXY}" in go_builder
    assert "RUNTIME_GO_PROXY" not in runtime_base
    assert "GOPROXY" not in runtime_base


def test_runtime_base_installs_both_projects_non_editably_before_closure_checks():
    text = DOCKERFILE.read_text(encoding="utf-8")
    runtime_base = _dockerfile_stage(text, "runtime-base")

    library_copy = runtime_base.index("COPY strategy-library/pyproject.toml")
    service_project_copy = runtime_base.index("COPY strategy-service/pyproject.toml")
    frozen_sync = runtime_base.index("uv sync --frozen --no-dev --no-editable")
    assert library_copy < frozen_sync
    assert service_project_copy < frozen_sync
    assert "--no-install-package" not in runtime_base
    assert "uv pip check --python /app/strategy-service/.venv/bin/python" in runtime_base
    assert "check_runtime_dependency_contract.py" in runtime_base
    assert "verify-installed" in runtime_base
    assert "session_worker_entry" in runtime_base
    assert "runtime_worker_pb2" in runtime_base
    assert "control_panel_service_pb2" in runtime_base
    assert "runtime_dependency_worker_smoke.py" in runtime_base
    assert "COPY strategy-service/tests/" not in runtime_base
    assert "COPY strategy-library/tests/" not in runtime_base


def test_final_images_do_not_shadow_installed_library_and_repeat_closure():
    text = DOCKERFILE.read_text(encoding="utf-8")
    runtime_base = _dockerfile_stage(text, "runtime-base")
    coverage = _dockerfile_stage(text, "executor-coverage")

    pythonpath_lines = [
        line for line in text.splitlines() if "PYTHONPATH=" in line
    ]
    assert all("/app/strategy-library" not in line for line in pythonpath_lines)
    assert "uv sync --frozen --no-dev --extra coverage --no-editable" in coverage
    assert "uv pip check --python /app/strategy-service/.venv/bin/python" in coverage
    assert "check_runtime_dependency_contract.py" in coverage
    assert "verify-installed" in coverage
    assert "runtime_dependency_worker_smoke.py" in coverage
    assert "--coverage false --check-only" in runtime_base
    assert "--coverage true --check-only" in coverage


def test_all_image_smokes_compare_the_inherited_runtime_identity_environment():
    text = DOCKERFILE.read_text(encoding="utf-8")
    runtime_base = _dockerfile_stage(text, "runtime-base")
    coverage = _dockerfile_stage(text, "executor-coverage")

    for stage in (runtime_base, coverage):
        assert '--expected-profile "${HUSHINE_RUNTIME_PROFILE_NAME}"' in stage
        assert '--expected-version "${HUSHINE_RUNTIME_PROFILE_VERSION}"' in stage
        assert '--expected-digest "${HUSHINE_RUNTIME_CONTRACT_SHA256}"' in stage
        assert '--expected-profile "${RUNTIME_PROFILE_NAME}"' not in stage
        assert '--expected-version "${RUNTIME_PROFILE_VERSION}"' not in stage
        assert '--expected-digest "${RUNTIME_CONTRACT_SHA256}"' not in stage


def test_final_targets_embed_all_runtime_identity_facts():
    text = DOCKERFILE.read_text(encoding="utf-8")
    expected_labels = {
        "org.hushine.runtime.profile",
        "org.hushine.runtime.profile.version",
        "org.hushine.runtime.contract.sha256",
        "org.hushine.runtime.strategy-service.commit",
        "org.hushine.runtime.strategy-library.commit",
        "org.hushine.runtime.golang-lib.commit",
        "org.hushine.runtime.core-service.commit",
        "org.hushine.runtime.image-build-id",
        "org.hushine.runtime.source-dirty",
        "org.hushine.runtime.source-state.sha256",
    }
    expected_env = {
        "HUSHINE_RUNTIME_PROFILE_NAME",
        "HUSHINE_RUNTIME_PROFILE_VERSION",
        "HUSHINE_RUNTIME_CONTRACT_SHA256",
        "HUSHINE_RUNTIME_HOSTED_PYTHON",
        "HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS",
        "HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT",
        "HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT",
        "HUSHINE_RUNTIME_GOLANG_LIB_COMMIT",
        "HUSHINE_RUNTIME_CORE_SERVICE_COMMIT",
        "HUSHINE_RUNTIME_IMAGE_BUILD_ID",
        "HUSHINE_RUNTIME_SOURCE_DIRTY",
        "HUSHINE_RUNTIME_SOURCE_STATE_SHA256",
    }
    for fact in expected_labels | expected_env:
        assert fact in text


def test_final_images_have_no_unlocked_install_or_public_root_allowlist():
    text = DOCKERFILE.read_text(encoding="utf-8")
    install_lines = [line.strip() for line in text.splitlines() if "uv sync" in line]
    assert install_lines == [
        "RUN uv sync --frozen --no-dev --no-editable \\",
        "RUN uv sync --frozen --no-dev --extra coverage --no-editable \\",
    ]
    assert "pip install" not in text
    public_probes = {
        dependency.probe
        for dependency in load_runtime_dependency_profile().dependencies
        if dependency.public
    }
    assert not any(
        set(line.removeprefix("ENV ").split()) == public_probes
        for line in text.splitlines()
    )


def test_representative_strategy_imports_are_generated_from_packaged_profile():
    body = FIXTURE.read_text(encoding="utf-8")
    source = representative_strategy_source(body)
    imported = {
        alias.name
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    profile = load_runtime_dependency_profile()

    assert imported == {
        dependency.probe for dependency in profile.dependencies if dependency.public
    }
    assert {name.split(".", 1)[0] for name in imported} == set(
        profile.public_import_roots
    )


def test_sdk_validator_can_be_imported_before_service_strategy_gate():
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "import hushine_strategy.validator; "
                "import strategy_service.strategy_imports"
            ),
        ],
        cwd=SERVICE_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_coverage_python_configuration_is_locked_and_image_scoped():
    pyproject = (SERVICE_DIR / "pyproject.toml").read_text(encoding="utf-8")
    coveragerc = (SERVICE_DIR / ".coveragerc").read_text(encoding="utf-8")

    assert 'coverage = [\n    "coverage>=7.0.0,<8.0.0",\n]' in pyproject
    assert "source = strategy_service" not in coveragerc
    assert "include = */strategy_service/*" in coveragerc
    assert "parallel = true" in coveragerc
    assert "sigterm = true" in coveragerc
    assert "disable_warnings = no-data-collected" in coveragerc
