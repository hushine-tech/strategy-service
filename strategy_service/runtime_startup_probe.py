from __future__ import annotations

import argparse
import hashlib
from importlib import metadata, util
import importlib
import json
from pathlib import Path
import re
import sys
from typing import Mapping, Sequence


SCHEMA_VERSION = 1
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}")
_DISTRIBUTIONS = (
    ("hushine-strategy-library", "hushine_strategy"),
    ("hushine-strategy-service", "strategy_service"),
)


def load_runtime_dependency_profile():
    from hushine_strategy.runtime_dependencies import (
        load_runtime_dependency_profile as load,
    )

    return load()


def probe_runtime_dependency_profile(*args, **kwargs):
    from hushine_strategy.runtime_dependencies import (
        probe_runtime_dependency_profile as probe,
    )

    return probe(*args, **kwargs)


def current_runtime_profile():
    from strategy_service.runtime_profile import current_runtime_profile as current

    return current()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "strict")).hexdigest()


def _failure(code: str, module: str, reason: str) -> dict[str, str]:
    # Callers pass literals or manifest-owned logical names only. Never pass an
    # exception string through this startup protocol.
    return {"code": code, "module": module, "reason": reason}


def _empty_profile() -> dict[str, object]:
    return {
        "schema_version": 0,
        "profile_name": "",
        "profile_version": "",
        "contract_sha256": "",
        "hosted_python": "",
        "public_import_roots": [],
        "strategy_service_commit": "",
        "strategy_library_commit": "",
        "image_build_id": "",
    }


def _profile_json(manifest, runtime_profile) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "profile_name": runtime_profile.name,
        "profile_version": runtime_profile.version,
        "contract_sha256": runtime_profile.contract_sha256,
        "hosted_python": runtime_profile.hosted_python,
        "public_import_roots": sorted(
            runtime_profile.allowed_third_party_modules
        ),
        "strategy_service_commit": runtime_profile.strategy_service_commit,
        "strategy_library_commit": runtime_profile.strategy_library_commit,
        "image_build_id": runtime_profile.image_build_id,
    }


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _distribution_direct_url(distribution) -> tuple[bool, bool]:
    raw = distribution.read_text("direct_url.json")
    if raw is None:
        return False, False
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        raise ValueError("invalid installed direct URL metadata") from None
    if (
        not isinstance(parsed, Mapping)
        or not isinstance(parsed.get("url"), str)
        or not parsed["url"]
    ):
        raise ValueError("invalid installed direct URL metadata")
    directory_info = parsed.get("dir_info")
    if directory_info is not None and not isinstance(directory_info, Mapping):
        raise ValueError("invalid installed direct URL metadata")
    if (
        isinstance(directory_info, Mapping)
        and "editable" in directory_info
        and type(directory_info["editable"]) is not bool
    ):
        raise ValueError("invalid installed direct URL metadata")
    editable = isinstance(directory_info, Mapping) and directory_info.get("editable") is True
    return True, editable


def _module_origin(module: str) -> Path:
    spec = util.find_spec(module)
    if spec is None:
        raise ModuleNotFoundError(module)
    if spec.origin and spec.origin not in {"built-in", "frozen"}:
        return Path(spec.origin).resolve()
    locations = tuple(spec.submodule_search_locations or ())
    if not locations:
        raise ModuleNotFoundError(module)
    return Path(locations[0]).resolve()


def _package_record(
    distribution_name: str,
    module: str,
    *,
    source: str,
) -> tuple[dict[str, object] | None, dict[str, str] | None]:
    try:
        distribution = metadata.distribution(distribution_name)
        version = distribution.version
        direct_url_present, editable = _distribution_direct_url(distribution)
        distribution_root = Path(distribution.locate_file("")).resolve()
        origin = _module_origin(module)
        prefix = Path(sys.prefix).resolve()
    except Exception:
        return None, _failure(
            "PACKAGE_METADATA_INVALID",
            distribution_name,
            "required installed package metadata is unavailable",
        )

    if not isinstance(version, str) or not version or len(version.encode("utf-8")) > 128:
        return None, _failure(
            "PACKAGE_METADATA_INVALID",
            distribution_name,
            "required installed package metadata is invalid",
        )
    if not _is_within(distribution_root, prefix):
        return None, _failure(
            "PACKAGE_ORIGIN_INVALID",
            module,
            "installed package metadata is outside the worker environment",
        )

    if editable:
        if source != "bare":
            return None, _failure(
                "EDITABLE_PACKAGE_FORBIDDEN",
                module,
                "editable packages are not allowed for this runtime source",
            )
        origin_kind = "editable"
    else:
        if not _is_within(origin, prefix):
            return None, _failure(
                "PACKAGE_ORIGIN_INVALID",
                module,
                "installed package origin is outside the worker environment",
            )
        origin_kind = "venv-site"

    return (
        {
            "distribution": distribution_name,
            "version": version,
            "direct_url_present": direct_url_present,
            "editable": editable,
            "origin_kind": origin_kind,
            "origin_sha256": _sha256_text(str(origin)),
        },
        None,
    )


def _base_result(source: str) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "ok": False,
        "source": source,
        "python_version": ".".join(str(part) for part in sys.version_info[:3]),
        "dependency_profile": _empty_profile(),
        "sys_prefix_sha256": _sha256_text(str(Path(sys.prefix).resolve())),
        "sys_executable_sha256": _sha256_text(sys.executable),
        "workdir_sha256": _sha256_text(str(Path.cwd().resolve())),
        "packages": [],
        "failures": [],
    }


def build_probe_result(
    *,
    source: str,
    expected_invocation_sha256: str,
    expected_workdir_sha256: str,
) -> dict[str, object]:
    result = _base_result(source)
    failures: list[dict[str, str]] = []

    if result["sys_executable_sha256"] != expected_invocation_sha256:
        failures.append(
            _failure(
                "INVOCATION_IDENTITY_MISMATCH",
                "sys.executable",
                "worker Python invocation identity did not match",
            )
        )
    if result["workdir_sha256"] != expected_workdir_sha256:
        failures.append(
            _failure(
                "WORKDIR_IDENTITY_MISMATCH",
                "os.getcwd",
                "worker working directory identity did not match",
            )
        )
    if Path(sys.prefix).resolve() == Path(sys.base_prefix).resolve():
        failures.append(
            _failure(
                "VIRTUAL_ENVIRONMENT_REQUIRED",
                "sys.prefix",
                "worker Python must run from an isolated virtual environment",
            )
        )

    try:
        # Import the neutral package before the application packages. This
        # catches packaging/circular-import regressions in the sealed image.
        importlib.import_module("hushine_runtime_import_probe")
        manifest = load_runtime_dependency_profile()
        runtime_profile = current_runtime_profile()
        result["dependency_profile"] = _profile_json(manifest, runtime_profile)
    except Exception:
        failures.append(
            _failure(
                "DEPENDENCY_PROBE_INITIALIZATION_FAILED",
                "hushine_strategy.runtime_dependencies",
                "runtime dependency probe could not be initialized",
            )
        )
        result["failures"] = sorted(
            failures, key=lambda item: (item["code"], item["module"], item["reason"])
        )
        return result

    if source != "bare" and (
        runtime_profile.strategy_service_commit == "local-dev"
        or runtime_profile.strategy_library_commit == "local-dev"
        or runtime_profile.image_build_id == "local-dev"
    ):
        failures.append(
            _failure(
                "IMAGE_IDENTITY_REQUIRED",
                "strategy_service.runtime_profile",
                "runtime image identity is required for this source",
            )
        )

    try:
        dependency_failures = probe_runtime_dependency_profile(
            manifest,
            python_executable=sys.executable,
            python_constraint=(
                manifest.debugger_python if source == "bare" else manifest.hosted_python
            ),
        )
    except Exception:
        failures.append(
            _failure(
                "DEPENDENCY_PROBE_INITIALIZATION_FAILED",
                "hushine_strategy.runtime_dependencies",
                "runtime dependency probe could not be initialized",
            )
        )
    else:
        for failure in dependency_failures:
            failures.append(
                _failure(
                    "DEPENDENCY_IMPORT_FAILED",
                    failure.probe,
                    "required runtime dependency probe failed",
                )
            )

    packages: list[dict[str, object]] = []
    for distribution_name, module in _DISTRIBUTIONS:
        record, failure = _package_record(
            distribution_name,
            module,
            source=source,
        )
        if record is not None:
            packages.append(record)
        if failure is not None:
            failures.append(failure)
    result["packages"] = packages
    result["failures"] = sorted(
        failures, key=lambda item: (item["code"], item["module"], item["reason"])
    )
    result["ok"] = not failures
    return result


def _emit_json(value: dict[str, object]) -> None:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    sys.stdout.write(encoded + "\n")
    sys.stdout.flush()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runtime_startup_probe")
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify")
    verify.add_argument(
        "--source", choices=("hosted", "self_hosted", "bare"), required=True
    )
    verify.add_argument(
        "--expected-invocation-sha256",
        type=_validated_sha256,
        required=True,
    )
    verify.add_argument(
        "--expected-workdir-sha256",
        type=_validated_sha256,
        required=True,
    )
    verify.add_argument("--json", action="store_true", required=True)
    return parser


def _validated_sha256(value: str) -> str:
    if _LOWER_SHA256.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("expected a lowercase SHA-256 digest")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    options = _parser().parse_args(argv)
    result = build_probe_result(
        source=options.source,
        expected_invocation_sha256=options.expected_invocation_sha256,
        expected_workdir_sha256=options.expected_workdir_sha256,
    )
    _emit_json(result)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
