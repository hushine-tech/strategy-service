from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from strategy_service import runtime_startup_probe


ROOT = Path(__file__).resolve().parents[1]
PYTHON = ROOT / ".venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
LOWER_SHA256 = "0" * 64
TOP_LEVEL_KEYS = {
    "schema_version",
    "ok",
    "source",
    "python_version",
    "dependency_profile",
    "sys_prefix_sha256",
    "sys_executable_sha256",
    "workdir_sha256",
    "packages",
    "failures",
}
PROFILE_KEYS = {
    "schema_version",
    "profile_name",
    "profile_version",
    "contract_sha256",
    "hosted_python",
    "public_import_roots",
    "strategy_service_commit",
    "strategy_library_commit",
    "image_build_id",
}
PACKAGE_KEYS = {
    "distribution",
    "version",
    "direct_url_present",
    "editable",
    "origin_kind",
    "origin_sha256",
}
FAILURE_KEYS = {"code", "module", "reason"}


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "strict")).hexdigest()


def _run_probe(*, source: str, executable: Path = PYTHON, cwd: Path = ROOT):
    return subprocess.run(
        [
            str(executable),
            "-I",
            "-m",
            "strategy_service.runtime_startup_probe",
            "verify",
            "--source",
            source,
            "--expected-invocation-sha256",
            _sha256_text(str(executable)),
            "--expected-workdir-sha256",
            _sha256_text(str(cwd.resolve())),
            "--json",
        ],
        cwd=cwd,
        env={
            "PATH": str(executable.parent),
            "HOME": str(cwd / ".test-home"),
        },
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def test_bare_probe_emits_one_exact_canonical_json_object():
    result = _run_probe(source="bare")

    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    assert result.stderr == b""
    payload = json.loads(result.stdout)
    assert set(payload) == TOP_LEVEL_KEYS
    assert set(payload["dependency_profile"]) == PROFILE_KEYS
    assert payload["source"] == "bare"
    assert payload["ok"] is True
    assert payload["python_version"].startswith("3.13.")
    assert payload["workdir_sha256"] == _sha256_text(str(ROOT.resolve()))
    assert result.stdout == (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")

    packages = payload["packages"]
    assert [item["distribution"] for item in packages] == [
        "hushine-strategy-library",
        "hushine-strategy-service",
    ]
    assert all(set(item) == PACKAGE_KEYS for item in packages)
    assert all(len(item["origin_sha256"]) == 64 for item in packages)
    assert {item["origin_kind"] for item in packages} <= {"editable", "venv-site"}


def test_probe_rejects_invocation_hash_mismatch_without_echoing_path(capsys):
    code = runtime_startup_probe.main(
        [
            "verify",
            "--source",
            "bare",
            "--expected-invocation-sha256",
            LOWER_SHA256,
            "--expected-workdir-sha256",
            _sha256_text(str(Path.cwd().resolve())),
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["ok"] is False
    assert set(payload["failures"][0]) == FAILURE_KEYS
    assert payload["failures"][0] == {
        "code": "INVOCATION_IDENTITY_MISMATCH",
        "module": "sys.executable",
        "reason": "worker Python invocation identity did not match",
    }
    assert str(Path.cwd()) not in captured.out
    assert sys.executable not in captured.out


@pytest.mark.parametrize("source", ["hosted", "self_hosted"])
def test_non_bare_probe_rejects_local_dev_image_facts(source, monkeypatch, capsys):
    monkeypatch.delenv("HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT", raising=False)
    monkeypatch.delenv("HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT", raising=False)
    monkeypatch.delenv("HUSHINE_RUNTIME_GOLANG_LIB_COMMIT", raising=False)
    monkeypatch.delenv("HUSHINE_RUNTIME_CORE_SERVICE_COMMIT", raising=False)
    monkeypatch.delenv("HUSHINE_RUNTIME_IMAGE_BUILD_ID", raising=False)
    monkeypatch.setattr(
        runtime_startup_probe,
        "_package_record",
        lambda distribution, module, *, source: (
            {
                "distribution": distribution,
                "version": "0.1.0",
                "direct_url_present": False,
                "editable": False,
                "origin_kind": "venv-site",
                "origin_sha256": LOWER_SHA256,
            },
            None,
        ),
    )

    code = runtime_startup_probe.main(
        [
            "verify",
            "--source",
            source,
            "--expected-invocation-sha256",
            _sha256_text(sys.executable),
            "--expected-workdir-sha256",
            _sha256_text(str(Path.cwd().resolve())),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["ok"] is False
    assert payload["failures"] == [
        {
            "code": "IMAGE_IDENTITY_REQUIRED",
            "module": "strategy_service.runtime_profile",
            "reason": "runtime image identity is required for this source",
        }
    ]


def test_failure_records_are_bounded_and_never_include_exception_details(
    monkeypatch, capsys
):
    canary = "/private/secret/path token=do-not-print\nsecond-line"

    def fail_profile(*_args, **_kwargs):
        raise ModuleNotFoundError(canary, name="internal_transitive_secret")

    monkeypatch.setattr(runtime_startup_probe, "probe_runtime_dependency_profile", fail_profile)
    monkeypatch.setattr(
        runtime_startup_probe,
        "_package_record",
        lambda distribution, module, *, source: (
            {
                "distribution": distribution,
                "version": "0.1.0",
                "direct_url_present": True,
                "editable": source == "bare",
                "origin_kind": "editable" if source == "bare" else "venv-site",
                "origin_sha256": LOWER_SHA256,
            },
            None,
        ),
    )
    code = runtime_startup_probe.main(
        [
            "verify",
            "--source",
            "bare",
            "--expected-invocation-sha256",
            _sha256_text(sys.executable),
            "--expected-workdir-sha256",
            _sha256_text(str(Path.cwd().resolve())),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 1
    assert canary not in output
    assert "internal_transitive_secret" not in output
    assert payload["failures"] == [
        {
            "code": "DEPENDENCY_PROBE_INITIALIZATION_FAILED",
            "module": "hushine_strategy.runtime_dependencies",
            "reason": "runtime dependency probe could not be initialized",
        }
    ]
    assert len(output.encode("utf-8")) < 64 * 1024


def test_cli_rejects_unknown_source_before_running_probe():
    completed = subprocess.run(
        [
            str(PYTHON),
            "-I",
            "-m",
            "strategy_service.runtime_startup_probe",
            "verify",
            "--source",
            "unknown",
            "--expected-invocation-sha256",
            LOWER_SHA256,
            "--expected-workdir-sha256",
            LOWER_SHA256,
            "--json",
        ],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    assert completed.returncode != 0
    assert completed.stdout == b""
