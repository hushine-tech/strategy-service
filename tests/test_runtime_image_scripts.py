from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess

import pytest


SERVICE_DIR = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = SERVICE_DIR / "scripts" / "build_strategy_runtime.sh"
CONTEXT_SCRIPT = SERVICE_DIR / "scripts" / "prepare_runtime_build_context.py"
VERIFY_SCRIPT = SERVICE_DIR / "scripts" / "verify_runtime_image.sh"
SMOKE_SCRIPT = SERVICE_DIR / "scripts" / "smoke_strategy_runtime.sh"
FAULT_DOCKERFILE = SERVICE_DIR / "tests" / "fixtures" / "Dockerfile.runtime-dependency-fault"


def _run(*args: str, cwd: Path, env: dict[str, str] | None = None):
    return subprocess.run(
        list(args),
        cwd=cwd,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def _git(repository: Path, *args: str) -> str:
    result = _run("git", *args, cwd=repository)
    result.check_returncode()
    return result.stdout.strip()


def _repository(root: Path, name: str, files: dict[str, str]) -> Path:
    repository = root / name
    repository.mkdir()
    _git(repository, "init", "-q")
    _git(repository, "config", "user.email", "runtime@example.invalid")
    _git(repository, "config", "user.name", "Runtime Test")
    for relative, content in files.items():
        path = repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    _git(repository, "add", ".")
    _git(repository, "commit", "-qm", "fixture")
    return repository


@pytest.fixture
def runtime_repositories(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    service = _repository(tmp_path, "strategy-service", {"service.py": "service\n"})
    library = _repository(tmp_path, "strategy-library", {"library.py": "library\n"})
    golang = _repository(tmp_path, "golang-lib", {"go.mod": "module fixture\n"})
    core = _repository(tmp_path, "core-service", {"gen/portfoliov1/portfolio.pb.go": "package portfoliov1\n"})
    for repository in (service, library, golang, core):
        (repository / ".gitignore").write_text(
            ".venv/\n.pytest_cache/\n__pycache__/\n*.egg-info/\n.coverage*\n",
            encoding="utf-8",
        )
        _git(repository, "add", ".gitignore")
        _git(repository, "commit", "-qm", "ignore host artifacts")
        for relative in (
            ".venv/ignored",
            ".pytest_cache/ignored",
            "__pycache__/ignored",
            "fixture.egg-info/ignored",
            ".coverage.ignored",
        ):
            path = repository / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("must-not-enter-context\n", encoding="utf-8")
    return service, library, golang, core


def _prepare_context(
    tmp_path: Path,
    repositories: tuple[Path, Path, Path, Path],
    *,
    allow_dirty: bool = False,
):
    index = len(tuple(tmp_path.glob("context-*")))
    context = tmp_path / f"context-{index}-{'dirty' if allow_dirty else 'clean'}"
    context.mkdir(mode=0o700)
    args = [
        "python3",
        str(CONTEXT_SCRIPT),
        "--output",
        str(context),
        "--service-repository",
        str(repositories[0]),
        "--library-repository",
        str(repositories[1]),
        "--golang-lib-repository",
        str(repositories[2]),
        "--core-repository",
        str(repositories[3]),
        "--profile-digest",
        "a" * 64,
    ]
    if allow_dirty:
        args.append("--allow-dirty")
    result = _run(*args, cwd=SERVICE_DIR)
    return result, context


def test_sealed_context_contains_only_git_derived_inputs(
    tmp_path: Path, runtime_repositories: tuple[Path, Path, Path, Path]
):
    result, context = _prepare_context(tmp_path, runtime_repositories)
    result.check_returncode()
    payload = json.loads(result.stdout)

    assert stat.S_IMODE(context.stat().st_mode) == 0o700
    assert payload["source_dirty"] is False
    assert len(payload["source_state_sha256"]) == 64
    assert (context / "strategy-service/service.py").read_text() == "service\n"
    assert (context / "strategy-library/library.py").read_text() == "library\n"
    assert (context / "golang-lib/go.mod").read_text() == "module fixture\n"
    assert (context / "core-service/gen/portfoliov1/portfolio.pb.go").read_text() == "package portfoliov1\n"
    assert payload["commits"]["core-service"] == _git(runtime_repositories[3], "rev-parse", "HEAD")
    staged_paths = tuple(context.rglob("*"))
    staged = [str(path.relative_to(context)) for path in staged_paths]
    assert not any(".git" in Path(path).parts for path in staged)
    assert not any(
        path.is_file() and not path.is_symlink() and b"must-not-enter-context" in path.read_bytes()
        for path in staged_paths
    )
    assert not any(
        marker in path
        for path in staged
        for marker in (".venv", "__pycache__", ".pytest_cache", ".egg-info", ".coverage")
    )


def test_clean_context_rejects_dirty_golang_lib_and_dirty_identity_changes(
    tmp_path: Path, runtime_repositories: tuple[Path, Path, Path, Path]
):
    clean, clean_context = _prepare_context(tmp_path, runtime_repositories)
    clean.check_returncode()
    clean_state = json.loads(clean.stdout)["source_state_sha256"]
    (runtime_repositories[2] / "go.mod").write_text("module changed\n", encoding="utf-8")

    rejected, _ = _prepare_context(tmp_path, runtime_repositories)
    assert rejected.returncode == 2
    assert "dirty" in rejected.stderr.lower()

    dirty, dirty_context = _prepare_context(
        tmp_path, runtime_repositories, allow_dirty=True
    )
    dirty.check_returncode()
    payload = json.loads(dirty.stdout)
    assert payload["source_dirty"] is True
    assert payload["source_state_sha256"] != clean_state
    assert (dirty_context / "golang-lib/go.mod").read_text() == "module changed\n"
    assert clean_context.exists()


def test_dirty_context_tracks_untracked_deletions_symlinks_and_executable_bits(
    tmp_path: Path, runtime_repositories: tuple[Path, Path, Path, Path]
):
    service = runtime_repositories[0]
    (service / "service.py").unlink()
    executable = service / "run.sh"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o755)
    (service / "link").symlink_to("run.sh")
    (service / "library-link").symlink_to("../strategy-library")

    result, context = _prepare_context(
        tmp_path, runtime_repositories, allow_dirty=True
    )
    result.check_returncode()
    assert not (context / "strategy-service/service.py").exists()
    assert os.access(context / "strategy-service/run.sh", os.X_OK)
    assert (context / "strategy-service/link").is_symlink()
    assert os.readlink(context / "strategy-service/link") == "run.sh"
    manifest = json.loads(
        (context / ".hushine-runtime-source-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    entries = {entry["path"]: entry for entry in manifest["entries"]}
    assert entries["strategy-service/library-link"] == {
        "executable": False,
        "path": "strategy-service/library-link",
        "target": "../strategy-library",
        "type": "symlink",
    }


def test_dirty_context_rejects_a_tracked_path_beneath_replaced_directory_symlink(
    tmp_path: Path, runtime_repositories: tuple[Path, Path, Path, Path]
):
    service = runtime_repositories[0]
    nested = service / "shared"
    nested.mkdir()
    (nested / "library.py").write_text("service-owned\n", encoding="utf-8")
    _git(service, "add", "shared/library.py")
    _git(service, "commit", "-qm", "track nested source")
    shutil.rmtree(nested)
    nested.symlink_to("../strategy-library", target_is_directory=True)

    result, _ = _prepare_context(
        tmp_path, runtime_repositories, allow_dirty=True
    )

    assert result.returncode == 2
    assert "symlink ancestor" in result.stderr.lower()


def _fake_docker(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.jsonl"
    docker = bin_dir / "docker"
    docker.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['DOCKER_LOG'], 'a', encoding='utf-8') as out:\n"
        "    out.write(json.dumps(args) + '\\n')\n"
        "state_path = os.environ['DOCKER_STATE']\n"
        "try:\n"
        "    state = json.load(open(state_path, encoding='utf-8'))\n"
        "except (FileNotFoundError, json.JSONDecodeError):\n"
        "    state = {}\n"
        "if args and args[0] == 'build':\n"
        "    facts = {}\n"
        "    tags = []\n"
        "    for index, value in enumerate(args):\n"
        "        if value == '--build-arg':\n"
        "            key, item = args[index + 1].split('=', 1)\n"
        "            facts[key] = item\n"
        "        elif value == '-t':\n"
        "            tags.append(args[index + 1])\n"
        "    for tag in tags:\n"
        "        state[tag] = facts\n"
        "    with open(state_path, 'w', encoding='utf-8') as out:\n"
        "        json.dump(state, out)\n"
        "elif args[:2] == ['image', 'inspect']:\n"
        "    facts = state[args[2]]\n"
        "    suffixes = {\n"
        "        'RUNTIME_PROFILE_NAME': 'profile',\n"
        "        'RUNTIME_PROFILE_VERSION': 'profile.version',\n"
        "        'RUNTIME_CONTRACT_SHA256': 'contract.sha256',\n"
        "        'RUNTIME_STRATEGY_SERVICE_COMMIT': 'strategy-service.commit',\n"
        "        'RUNTIME_STRATEGY_LIBRARY_COMMIT': 'strategy-library.commit',\n"
        "        'RUNTIME_GOLANG_LIB_COMMIT': 'golang-lib.commit',\n"
        "        'RUNTIME_CORE_SERVICE_COMMIT': 'core-service.commit',\n"
        "        'RUNTIME_IMAGE_BUILD_ID': 'image-build-id',\n"
        "        'RUNTIME_SOURCE_DIRTY': 'source-dirty',\n"
        "        'RUNTIME_SOURCE_STATE_SHA256': 'source-state.sha256',\n"
        "    }\n"
        "    labels = {'org.hushine.runtime.' + suffix: facts[key] for key, suffix in suffixes.items()}\n"
        "    env = ['HUSHINE_' + key + '=' + value for key, value in facts.items()]\n"
        "    print(json.dumps([{'Config': {'Labels': labels, 'Env': env}}]))\n"
        "sys.exit(0)\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return bin_dir, log


def _build(
    tmp_path: Path,
    *arguments: str,
    extra_env: dict[str, str] | None = None,
):
    bin_dir, log = _fake_docker(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
    env["DOCKER_LOG"] = str(log)
    env["DOCKER_STATE"] = str(tmp_path / "docker-state.json")
    env["IMAGE_PREFIX"] = "hushine/strategy-runtime"
    env.update(extra_env or {})
    result = _run(str(BUILD_SCRIPT), *arguments, cwd=SERVICE_DIR, env=env)
    calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    return result, calls


def _option_values(arguments: list[str], option: str) -> list[str]:
    return [
        arguments[index + 1]
        for index, value in enumerate(arguments)
        if value == option
    ]


@pytest.mark.parametrize("mode,target,tag", [
    ((), "executor", "hushine/strategy-runtime:executor-v1.2.3"),
    (("--coverage",), "executor-coverage", "hushine/strategy-runtime:executor-coverage-v1.2.3"),
])
def test_build_script_uses_sealed_context_and_target_identity(
    tmp_path: Path, mode: tuple[str, ...], target: str, tag: str
):
    result, calls = _build(tmp_path, *mode, "--allow-dirty", "v1.2.3")
    result.check_returncode()
    builds = [call for call in calls if call and call[0] == "build"]
    assert len(builds) == 1
    build = builds[0]
    assert _option_values(build, "--target") == [target]
    assert tag in _option_values(build, "-t")
    assert str(SERVICE_DIR.parent) not in build
    context = Path(build[-1])
    assert context.name.startswith("hushine-runtime-context-")
    assert not context.exists()
    build_args = dict(
        value.split("=", 1)
        for value in _option_values(build, "--build-arg")
    )
    assert build_args["RUNTIME_IMAGE_BUILD_ID"].endswith(target) or (
        f"{target}-dirty-" in build_args["RUNTIME_IMAGE_BUILD_ID"]
    )
    for key in (
        "RUNTIME_PROFILE_NAME",
        "RUNTIME_PROFILE_VERSION",
        "RUNTIME_CONTRACT_SHA256",
        "RUNTIME_HOSTED_PYTHON",
        "RUNTIME_PUBLIC_IMPORT_ROOTS",
        "RUNTIME_STRATEGY_SERVICE_COMMIT",
        "RUNTIME_STRATEGY_LIBRARY_COMMIT",
        "RUNTIME_GOLANG_LIB_COMMIT",
        "RUNTIME_CORE_SERVICE_COMMIT",
        "RUNTIME_SOURCE_DIRTY",
        "RUNTIME_SOURCE_STATE_SHA256",
    ):
        assert build_args[key]


def test_build_script_passes_validated_build_only_go_proxy(tmp_path: Path):
    proxy = "https://goproxy.cn,direct"
    result, calls = _build(
        tmp_path,
        "--allow-dirty",
        "proxy-contract",
        extra_env={"RUNTIME_GO_PROXY": proxy},
    )

    result.check_returncode()
    build = next(call for call in calls if call and call[0] == "build")
    build_args = dict(
        value.split("=", 1)
        for value in _option_values(build, "--build-arg")
    )
    assert build_args["RUNTIME_GO_PROXY"] == proxy


@pytest.mark.parametrize(
    "proxy",
    ["http://goproxy.example", "https://user@goproxy.example", "https://bad proxy"],
)
def test_build_script_rejects_unsafe_go_proxy_before_docker(
    tmp_path: Path, proxy: str
):
    result, calls = _build(
        tmp_path,
        "--allow-dirty",
        "proxy-contract",
        extra_env={"RUNTIME_GO_PROXY": proxy},
    )

    assert result.returncode == 2
    assert calls == []
    assert "invalid RUNTIME_GO_PROXY" in result.stderr


def test_all_builds_targets_separately_with_distinct_ids(tmp_path: Path):
    result, calls = _build(tmp_path, "--all", "--allow-dirty", "contract")
    result.check_returncode()
    builds = [call for call in calls if call and call[0] == "build"]
    assert len(builds) == 2
    assert [_option_values(call, "--target") for call in builds] == [
        ["executor"],
        ["executor-coverage"],
    ]
    ids = [
        dict(value.split("=", 1) for value in _option_values(call, "--build-arg"))[
            "RUNTIME_IMAGE_BUILD_ID"
        ]
        for call in builds
    ]
    assert ids[0] != ids[1]


def test_all_verify_invokes_exact_final_image_gates(tmp_path: Path):
    result, calls = _build(
        tmp_path,
        "--all",
        "--verify",
        "--allow-dirty",
        "contract",
    )
    result.check_returncode()
    inspections = [call for call in calls if call[:2] == ["image", "inspect"]]
    assert inspections == [
        ["image", "inspect", "hushine/strategy-runtime:executor-contract"],
        ["image", "inspect", "hushine/strategy-runtime:executor-coverage-contract"],
    ]
    runs = [call for call in calls if call and call[0] == "run"]
    assert len(runs) == 10
    representative = [
        call
        for call in runs
        if any(value.endswith("/runtime_dependency_worker_smoke.py") for value in call)
    ]
    assert len(representative) == 2
    assert [
        call[call.index("--coverage") + 1] for call in representative
    ] == ["false", "true"]
    assert all("--check-only" in call for call in representative)


@pytest.mark.parametrize("arguments", [
    (),
    ("coverage",),
    ("--unknown", "v1"),
    ("--coverage",),
    ("--all", "--coverage", "v1"),
])
def test_invalid_build_invocations_exit_two_before_docker(
    tmp_path: Path, arguments: tuple[str, ...]
):
    result, calls = _build(tmp_path, *arguments)
    assert result.returncode == 2
    assert calls == []


@pytest.mark.parametrize(
    "variable,value",
    [
        ("IMAGE_BUILD_ID", ""),
        ("IMAGE_BUILD_ID", "contains whitespace"),
        ("IMAGE_BUILD_ID", "build-1"),
        ("EXECUTOR_IMAGE_BUILD_ID", ""),
    ],
)
def test_explicit_build_id_rejects_empty_or_unsafe_values(
    tmp_path: Path, variable: str, value: str
):
    result, calls = _build(
        tmp_path,
        "--allow-dirty",
        "contract",
        extra_env={variable: value},
    )
    assert result.returncode == 2
    assert calls == []


def test_verifier_and_smoke_require_explicit_named_contract_arguments():
    for script in (VERIFY_SCRIPT, SMOKE_SCRIPT):
        result = _run(str(script), cwd=SERVICE_DIR)
        assert result.returncode == 2


def test_fault_fixture_uninstalls_named_distribution_without_changing_contract():
    content = FAULT_DOCKERFILE.read_text(encoding="utf-8")
    assert "ARG BASE_IMAGE" in content
    assert "ARG FAULT_DISTRIBUTION" in content
    assert "uv pip uninstall" in content
    assert "verify-installed" in content
    assert "AS build-gate" in content
    assert "AS startup-gate" in content
    assert "runtime_dependencies.toml" not in content
    assert "strategy_validator" not in content
    from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile

    assert not any(
        dependency.distribution in content
        for dependency in load_runtime_dependency_profile().dependencies
        if dependency.public
    )


def test_makefile_exposes_release_and_development_image_gates():
    content = (SERVICE_DIR / "Makefile").read_text(encoding="utf-8")
    assert '$$HOME/.local/bin/uv' in content
    assert "export UV" in content
    assert '"$(UV)" sync --python 3.13 --frozen --extra dev' in content
    assert 'PYTHONPATH=../strategy-library "$(UV)" run --frozen' in content
    assert "runtime-images:" in content
    assert "build_strategy_runtime.sh --all --allow-dirty dev" in content
    assert "runtime-images-verify:" in content
    assert "build_strategy_runtime.sh --all --no-cache --verify contract" in content
    assert "runtime-images-verify-dev:" in content
    assert "--all --no-cache --verify --allow-dirty contract" in content


def test_makefile_honors_uv_bin_for_direct_managed_commands(tmp_path: Path):
    uv = tmp_path / "portable uv"
    uv.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    uv.chmod(0o700)
    env = {
        **os.environ,
        "HOME": str(tmp_path / "empty-home"),
        "PATH": str(tmp_path / "empty-path"),
        "UV_BIN": str(uv),
        "UV": str(tmp_path / "must-not-run-uv"),
    }

    result = _run(
        shutil.which("make") or "make",
        "-n",
        "dependency-contract",
        cwd=SERVICE_DIR,
        env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert f'"{uv}" sync --python 3.13' in result.stdout


def test_windows_native_acceptance_resolves_managed_uv_portably():
    script = (
        SERVICE_DIR / "scripts" / "runtime-agent-windows-native.test.ps1"
    ).read_text(encoding="utf-8")

    assert "function Resolve-UvExecutable" in script
    assert "$env:UV_BIN" in script
    assert "$env:UV" in script
    assert ".local\\bin\\$Name" in script
    assert '"uv.exe"' in script
    assert "& $UvExecutable" in script
    assert "[Environment]::OSVersion.Platform" in script
    assert "$IsWindows" not in script
    assert "\n    uv sync " not in script
    assert "\n    uv run " not in script
