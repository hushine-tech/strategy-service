from __future__ import annotations

import re
from pathlib import Path
import tomllib

from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile
from scripts.check_runtime_dependency_contract import (
    BEGIN_MARKER,
    END_MARKER,
    ContractViolation,
    check_project_projection,
    sync_project_projection,
)


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "pyproject.toml"
LOCK = ROOT / "uv.lock"
MAKEFILE = ROOT / "Makefile"
PROFILE = load_runtime_dependency_profile()


def _normalize(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _requirement_name(requirement: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)", requirement)
    assert match is not None
    return _normalize(match.group(1))


def _direct_names(path: Path) -> tuple[str, ...]:
    project = tomllib.loads(path.read_text(encoding="utf-8"))
    return tuple(_requirement_name(item) for item in project["project"]["dependencies"])


def _locked_names(path: Path) -> tuple[str, ...]:
    lock = tomllib.loads(path.read_text(encoding="utf-8"))
    return tuple(_normalize(package["name"]) for package in lock["package"])


def _public_names() -> tuple[str, ...]:
    return tuple(_normalize(item) for item in PROFILE.public_distributions)


def _write_projection_fixture(
    tmp_path: Path,
    *,
    generated: tuple[str, ...],
    outside: tuple[str, ...] = (),
) -> tuple[Path, Path]:
    project = tmp_path / "pyproject.toml"
    project.write_text(
        "\n".join(
            [
                "[project]",
                'name = "projection-fixture"',
                'version = "0.1.0"',
                'requires-python = ">=3.13"',
                "dependencies = [",
                *(f'    "{item}",' for item in outside),
                f"    {BEGIN_MARKER}",
                *(f'    "{item}",' for item in generated),
                f"    {END_MARKER}",
                "]",
                "",
            ]
        ),
        encoding="utf-8",
    )
    lock = tmp_path / "uv.lock"
    lock.write_text(
        "version = 1\nrevision = 3\nrequires-python = \">=3.13\"\n"
        + "".join(
            "\n[[package]]\n"
            f'name = "{distribution}"\n'
            'version = "1.0.0"\n'
            'source = { registry = "https://pypi.org/simple" }\n'
            for distribution in PROFILE.public_distributions
        ),
        encoding="utf-8",
    )
    return project, lock


def _has_violation(
    violations: tuple[ContractViolation, ...],
    code: str,
    distribution: str,
) -> bool:
    normalized = _normalize(distribution)
    return any(
        item.code == code and _normalize(item.distribution) == normalized
        for item in violations
    )


def test_every_public_distribution_is_direct_and_locked() -> None:
    public = set(_public_names())
    direct = set(_direct_names(PROJECT))
    locked = set(_locked_names(LOCK))

    assert public - direct == set()
    assert public - locked == set()
    assert check_project_projection(
        PROFILE,
        "strategy-service",
        PROJECT,
        LOCK,
    ) == ()


def test_generated_projection_is_a_check_mode_noop() -> None:
    before = PROJECT.read_bytes()

    assert sync_project_projection(PROFILE, PROJECT) == ()
    assert PROJECT.read_bytes() == before


def test_transitive_lock_entry_does_not_replace_a_direct_dependency(tmp_path: Path) -> None:
    selected = PROFILE.public_distributions[0]
    generated = tuple(
        item for item in PROFILE.public_distributions if item != selected
    )
    project, lock = _write_projection_fixture(tmp_path, generated=generated)

    violations = check_project_projection(
        PROFILE,
        "strategy-service-fixture",
        project,
        lock,
    )

    assert _has_violation(violations, "MISSING_DIRECT_DISTRIBUTION", selected)


def test_public_dependency_outside_marker_has_a_distinct_violation(tmp_path: Path) -> None:
    selected = PROFILE.public_distributions[0]
    generated = tuple(
        item for item in PROFILE.public_distributions if item != selected
    )
    project, lock = _write_projection_fixture(
        tmp_path,
        generated=generated,
        outside=(selected,),
    )
    before = project.read_bytes()

    violations = check_project_projection(
        PROFILE,
        "strategy-service-fixture",
        project,
        lock,
    )

    assert _has_violation(
        violations,
        "PUBLIC_DISTRIBUTION_OUTSIDE_PROJECTION",
        selected,
    )
    assert project.read_bytes() == before


def test_strategy_library_is_a_non_editable_sibling_source() -> None:
    project = tomllib.loads(PROJECT.read_text(encoding="utf-8"))
    dependencies = project["project"]["dependencies"]
    source = project["tool"]["uv"]["sources"]["hushine-strategy-library"]

    assert any(
        item.startswith("hushine-strategy-library>=") for item in dependencies
    )
    assert source == {"path": "../strategy-library"}
    assert "git+ssh://git@github.com/hushine-tech/strategy-library.git@main" not in PROJECT.read_text(
        encoding="utf-8"
    )

    lock = tomllib.loads(LOCK.read_text(encoding="utf-8"))
    library = next(
        package
        for package in lock["package"]
        if _normalize(package["name"]) == "hushine-strategy-library"
    )
    assert library["source"] == {"directory": "../strategy-library"}


def test_makefile_exposes_the_installed_dependency_contract_gate() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")

    assert "dependency-contract:" in makefile
    assert '"$(UV)" sync --python 3.13 --frozen --extra dev' in makefile
    assert "--service-project pyproject.toml --service-lock uv.lock" in makefile
    assert "--installed-python strategy-service=.venv/bin/python" in makefile
    assert "--installed-python-version strategy-service=3.13" in makefile
