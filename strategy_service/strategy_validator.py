from __future__ import annotations

import ast
import sys
from dataclasses import dataclass

from strategy_service.inputs import (
    StrategyDeclarationError,
    parse_declared_inputs,
    parse_order_targets,
)
from strategy_service.runtime_profile import DEBUGGER_ONLY_MODULES, current_runtime_profile

PUBLIC_PLATFORM_MODULES = {"strategy_service.types"}
PUBLIC_STRATEGY_SDK_MODULE = "hushine_strategy"
REQUIRED_ORDER_DECISION_FIELDS = {
    "exchange",
    "market",
    "symbol",
    "side",
    "qty",
    "order_type",
}
KNOWN_PHASE3_CONSTANTS = {
    ("Exchange", "BINANCE"): "binance",
    ("Exchange", "OKX"): "okx",
    ("Market", "SPOT"): "spot",
    ("Market", "PERPETUAL_FUTURES"): "perpetual_futures",
    ("Market", "DELIVERY_FUTURES"): "delivery_futures",
    ("OrderSide", "BUY"): "BUY",
    ("OrderSide", "SELL"): "SELL",
    ("OrderType", "MARKET"): "MARKET",
    ("OrderType", "LIMIT"): "LIMIT",
    ("PositionSide", "BOTH"): "BOTH",
    ("PositionSide", "LONG"): "LONG",
    ("PositionSide", "SHORT"): "SHORT",
}
VALID_ORDER_SIDES = {"BUY", "SELL"}
VALID_ORDER_TYPES = {"MARKET", "LIMIT"}


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

    _validate_phase3_contract(tree, issues)
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


def _validate_phase3_contract(tree: ast.AST, issues: list[StrategyValidationIssue]) -> None:
    strategy_class = _find_strategy_class(tree)
    if strategy_class is None:
        issues.append(
            StrategyValidationIssue(
                code="missing_strategy_class",
                message="strategy code must define class MyStrategy",
            )
        )
        return

    if not any(isinstance(node, ast.FunctionDef) and node.name == "on_market_data" for node in strategy_class.body):
        issues.append(
            StrategyValidationIssue(
                code="missing_on_market_data",
                message="MyStrategy must define on_market_data(self, data, wallet)",
                line=getattr(strategy_class, "lineno", 0),
            )
        )

    assignments = _class_assignments(strategy_class)
    if "INPUTS" not in assignments:
        issues.append(
            StrategyValidationIssue(
                code="missing_inputs",
                message="MyStrategy.INPUTS must declare at least one market data stream",
                line=getattr(strategy_class, "lineno", 0),
            )
        )
    else:
        _validate_inputs_literal(assignments["INPUTS"], issues)

    if "ORDER_TARGETS" not in assignments:
        issues.append(
            StrategyValidationIssue(
                code="missing_order_targets",
                message="MyStrategy.ORDER_TARGETS must declare tradable routes, or [] for read-only strategies",
                line=getattr(strategy_class, "lineno", 0),
            )
        )
    else:
        _validate_order_targets_literal(assignments["ORDER_TARGETS"], issues)

    for node in ast.walk(strategy_class):
        if isinstance(node, ast.Call) and _is_order_decision_call(node):
            _validate_order_decision_call(node, issues)


def _find_strategy_class(tree: ast.AST) -> ast.ClassDef | None:
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef) and node.name == "MyStrategy":
            return node
    return None


def _class_assignments(node: ast.ClassDef) -> dict[str, ast.AST]:
    assignments: dict[str, ast.AST] = {}
    for child in node.body:
        if isinstance(child, ast.Assign):
            for target in child.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = child.value
        elif isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
            assignments[child.target.id] = child.value
    return assignments


def _validate_inputs_literal(node: ast.AST, issues: list[StrategyValidationIssue]) -> None:
    try:
        raw = _literal_eval_phase3(node)
        parse_declared_inputs(raw)
    except (ValueError, StrategyDeclarationError) as exc:
        issues.append(
            StrategyValidationIssue(
                code="invalid_inputs",
                message=f"invalid INPUTS declaration: {exc}",
                line=getattr(node, "lineno", 0),
            )
        )


def _validate_order_targets_literal(node: ast.AST, issues: list[StrategyValidationIssue]) -> None:
    try:
        raw = _literal_eval_phase3(node)
        parse_order_targets(raw)
    except (ValueError, StrategyDeclarationError) as exc:
        issues.append(
            StrategyValidationIssue(
                code="invalid_order_targets",
                message=f"invalid ORDER_TARGETS declaration: {exc}",
                line=getattr(node, "lineno", 0),
            )
        )


def _literal_eval_phase3(node: ast.AST):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.List):
        return [_literal_eval_phase3(item) for item in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_literal_eval_phase3(item) for item in node.elts)
    if isinstance(node, ast.Dict):
        return {
            _literal_eval_phase3(key): _literal_eval_phase3(value)
            for key, value in zip(node.keys, node.values, strict=True)
        }
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        value = _literal_eval_phase3(node.operand)
        if isinstance(value, (int, float)):
            return -value
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and (node.value.id, node.attr) in KNOWN_PHASE3_CONSTANTS
    ):
        return KNOWN_PHASE3_CONSTANTS[(node.value.id, node.attr)]
    raise ValueError("declaration must be a literal or supported Phase 3 constant")


def _try_literal_eval_phase3(node: ast.AST) -> tuple[bool, object | None]:
    try:
        return True, _literal_eval_phase3(node)
    except ValueError:
        return False, None


def _is_order_decision_call(node: ast.Call) -> bool:
    if isinstance(node.func, ast.Name):
        return node.func.id == "OrderDecision"
    if isinstance(node.func, ast.Attribute):
        return node.func.attr == "OrderDecision"
    return False


def _validate_order_decision_call(node: ast.Call, issues: list[StrategyValidationIssue]) -> None:
    if node.args:
        _append_order_decision_issue(
            issues,
            node,
            "OrderDecision must use Phase 3 keyword arguments only",
        )
        return

    if any(keyword.arg is None for keyword in node.keywords):
        _append_order_decision_issue(
            issues,
            node,
            "OrderDecision cannot use **kwargs because the Phase 3 contract cannot be validated",
        )
        return

    keywords = {str(keyword.arg): keyword.value for keyword in node.keywords if keyword.arg}
    missing = sorted(REQUIRED_ORDER_DECISION_FIELDS - set(keywords))
    if missing:
        _append_order_decision_issue(
            issues,
            node,
            f"OrderDecision missing required fields: {', '.join(missing)}",
        )
        return

    for field in ("exchange", "market"):
        ok, value = _try_literal_eval_phase3(keywords[field])
        if ok:
            try:
                if field == "exchange":
                    parse_order_targets([{"exchange": value, "market": "perpetual_futures", "symbol": "BTCUSDT"}])
                else:
                    parse_order_targets([{"exchange": "binance", "market": value, "symbol": "BTCUSDT"}])
            except (ValueError, StrategyDeclarationError) as exc:
                _append_order_decision_issue(issues, node, f"invalid OrderDecision {field}: {exc}")
                return

    ok, side = _try_literal_eval_phase3(keywords["side"])
    if ok and str(side) not in VALID_ORDER_SIDES:
        _append_order_decision_issue(issues, node, "OrderDecision side must be BUY or SELL")
        return

    ok, order_type = _try_literal_eval_phase3(keywords["order_type"])
    if ok and str(order_type) not in VALID_ORDER_TYPES:
        _append_order_decision_issue(issues, node, "OrderDecision order_type must be MARKET or LIMIT")
        return

    for field in ("qty", "price"):
        if field not in keywords:
            continue
        ok, value = _try_literal_eval_phase3(keywords[field])
        if ok and value is not None and not isinstance(value, str):
            _append_order_decision_issue(issues, node, f"OrderDecision {field} must be a string")
            return


def _append_order_decision_issue(
    issues: list[StrategyValidationIssue],
    node: ast.AST,
    message: str,
) -> None:
    issues.append(
        StrategyValidationIssue(
            code="invalid_order_decision",
            message=message,
            line=getattr(node, "lineno", 0),
        )
    )
