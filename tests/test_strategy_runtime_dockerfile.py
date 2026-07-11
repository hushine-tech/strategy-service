from __future__ import annotations

import os
from pathlib import Path
import subprocess


SERVICE_DIR = Path(__file__).resolve().parents[1]
DOCKERFILE = SERVICE_DIR / "Dockerfile"
BUILD_SCRIPT = SERVICE_DIR / "scripts" / "build_strategy_runtime.sh"


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


def _run_build_script(
    tmp_path: Path, *script_args: str
) -> tuple[subprocess.CompletedProcess[str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/usr/bin/env bash\n"
        'printf \'%s\\n\' "$@" > "${DOCKER_ARGS_FILE}"\n',
        encoding="utf-8",
    )
    docker.chmod(0o755)

    args_file = tmp_path / "docker-args"
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["DOCKER_ARGS_FILE"] = str(args_file)
    env["IMAGE_PREFIX"] = "hushine/strategy-runtime"
    result = subprocess.run(
        [str(BUILD_SCRIPT), *script_args],
        cwd=SERVICE_DIR,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    return result, args_file


def _recorded_docker_args(tmp_path: Path, *script_args: str) -> list[str]:
    result, args_file = _run_build_script(tmp_path, *script_args)
    result.check_returncode()
    return args_file.read_text(encoding="utf-8").splitlines()


def _option_values(args: list[str], option: str) -> list[str]:
    return [args[index + 1] for index, value in enumerate(args) if value == option]


def test_strategy_runtime_image_builds_executor_only():
    content = DOCKERFILE.read_text(encoding="utf-8")

    assert "FROM python:3.13-slim AS runtime-base" in content
    assert "FROM runtime-base AS executor" in content
    assert "FROM executor AS default" in content
    assert "AS debugger" not in content
    assert "debugpy" not in content
    assert "pydevd-pycharm" not in content


def test_coverage_target_is_isolated_from_the_production_binary():
    text = DOCKERFILE.read_text(encoding="utf-8")
    go_builder = _dockerfile_stage(text, "go-builder")
    runtime_base = _dockerfile_stage(text, "runtime-base")
    coverage_builder = _dockerfile_stage(text, "go-coverage-builder")
    executor = _dockerfile_stage(text, "executor")
    coverage_executor = _dockerfile_stage(text, "executor-coverage")
    command = 'CMD ["./bin/runtime-agent", "--config", "config.yaml"]'

    assert "go build -o /out/runtime-agent ./cmd/runtime-agent" in go_builder
    assert "-cover" not in go_builder
    assert "COPY --from=go-builder /out/runtime-agent" in runtime_base
    assert "go build -cover -covermode=atomic -coverpkg=./..." in coverage_builder
    assert "COPY --from=go-coverage-builder /out/runtime-agent" in coverage_executor
    assert "uv sync --frozen --no-dev --extra coverage" in coverage_executor
    assert "COPY strategy-service/.coveragerc" in coverage_executor
    assert command in executor
    assert command in coverage_executor
    assert text.rstrip().endswith("FROM executor AS default")


def test_coverage_build_uses_only_the_dedicated_target_and_tag(tmp_path: Path):
    args = _recorded_docker_args(tmp_path, "--coverage")

    assert _option_values(args, "--target") == ["executor-coverage"]
    assert _option_values(args, "-t") == [
        "hushine/strategy-runtime:executor-coverage"
    ]


def test_bare_coverage_version_is_rejected_before_docker(tmp_path: Path):
    result, args_file = _run_build_script(tmp_path, "coverage")

    assert result.returncode != 0
    assert "version 'coverage' is reserved; use --coverage" in result.stderr
    assert not args_file.exists()


def test_normal_build_preserves_existing_targets_and_tags(tmp_path: Path):
    args = _recorded_docker_args(tmp_path, "v1.2.3")

    assert _option_values(args, "--target") == ["executor"]
    assert _option_values(args, "-t") == [
        "hushine/strategy-runtime:executor-v1.2.3",
        "hushine/strategy-runtime:executor",
        "hushine/strategy-runtime:dev",
        "hushine/strategy-runtime:v1.2.3",
    ]


def test_coverage_python_configuration_is_locked_and_image_scoped():
    pyproject = (SERVICE_DIR / "pyproject.toml").read_text(encoding="utf-8")
    coveragerc = (SERVICE_DIR / ".coveragerc").read_text(encoding="utf-8")

    assert 'coverage = [\n    "coverage>=7.0.0,<8.0.0",\n]' in pyproject
    assert "source = strategy_service" in coveragerc
    assert "parallel = true" in coveragerc
    assert "sigterm = true" in coveragerc
