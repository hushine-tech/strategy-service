from __future__ import annotations

import ast
import sys
from dataclasses import dataclass

from strategy_service.runtime_profile import DEBUGGER_ONLY_MODULES, current_runtime_profile

PUBLIC_PLATFORM_MODULES = {"strategy_service.types"}
PUBLIC_STRATEGY_SDK_MODULE = "hushine_strategy"


@dataclass(frozen=True)
class StrategyValidationIssue:
    code: str
    message: str
    module: str = ""
    line: int = 0


@dataclass(frozen=True)
class StrategyValidationResult:
    ok: bool
    issues: list[StrategyValidationIssue]
    runtime_version: str
    runtime_profile: str
    allowed_third_party_modules: list[str]


def validate_strategy_code(code: str) -> StrategyValidationResult:
    profile = current_runtime_profile()
    issues: list[StrategyValidationIssue] = []
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        issues.append(
            StrategyValidationIssue(
                code="syntax_error",
                message=exc.msg,
                line=exc.lineno or 0,
            )
        )
        return _result(False, issues)

    stdlib = _stdlib_modules()
    allowed = set(profile.allowed_third_party_modules)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _validate_module(alias.name, getattr(node, "lineno", 0), stdlib, allowed, issues)
        elif isinstance(node, ast.ImportFrom) and node.module:
            _validate_module(node.module, getattr(node, "lineno", 0), stdlib, allowed, issues)

    return _result(len(issues) == 0, issues)


def _result(ok: bool, issues: list[StrategyValidationIssue]) -> StrategyValidationResult:
    profile = current_runtime_profile()
    return StrategyValidationResult(
        ok=ok,
        issues=issues,
        runtime_version=profile.version,
        runtime_profile=profile.name,
        allowed_third_party_modules=sorted(profile.allowed_third_party_modules),
    )


def _root_module(name: str) -> str:
    return name.split(".", 1)[0]


def _stdlib_modules() -> set[str]:
    modules = set(getattr(sys, "stdlib_module_names", set()))
    modules.update({"__future__", "typing"})
    return modules


def _validate_module(
    import_name: str,
    line: int,
    stdlib: set[str],
    allowed: set[str],
    issues: list[StrategyValidationIssue],
) -> None:
    module_name = _root_module(import_name)
    if not module_name:
        return
    if (
        import_name in PUBLIC_PLATFORM_MODULES
        or import_name == PUBLIC_STRATEGY_SDK_MODULE
        or import_name.startswith(f"{PUBLIC_STRATEGY_SDK_MODULE}.")
    ):
        return
    if module_name in DEBUGGER_ONLY_MODULES:
        issues.append(
            StrategyValidationIssue(
                code="debugger_dependency_not_allowed",
                module=module_name,
                line=line,
                message=f"debugger module {module_name!r} cannot be used in saved strategy code",
            )
        )
        return
    if module_name in stdlib or module_name in allowed:
        return
    issues.append(
        StrategyValidationIssue(
            code="unsupported_dependency",
            module=module_name,
            line=line,
            message=f"module {module_name!r} is not part of the platform runtime profile",
        )
    )
