from __future__ import annotations

import ast
import sys
from dataclasses import dataclass

from hushine_strategy.import_validation import (
    HOSTED_PLATFORM_IMPORT_POLICY,
    validate_dependency_imports,
    validate_dynamic_import_safety,
    validate_platform_import_safety,
)
from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile
from strategy_service.inputs import (
    StrategyDeclarationError,
    parse_declared_inputs,
    parse_order_targets,
    parse_risk_controls,
    resolve_order_target_leverages,
)
from strategy_service.runtime_profile import current_runtime_profile

_DEPENDENCY_PROFILE = load_runtime_dependency_profile()
_HOSTED_PLATFORM_MODULES = frozenset(
    module for module, _ in HOSTED_PLATFORM_IMPORT_POLICY.allowed_from_symbols
)
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
    symbol: str = ""
    line: int = 0


@dataclass(frozen=True)
class StrategyValidationResult:
    ok: bool
    issues: list[StrategyValidationIssue]
    runtime_version: str
    runtime_profile: str
    allowed_third_party_modules: list[str]


def validate_strategy_code(code: str) -> StrategyValidationResult:
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

    relative_issues: list[StrategyValidationIssue] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level > 0:
            module = f"{'.' * node.level}{node.module or ''}"
            relative_issues.append(
                StrategyValidationIssue(
                    code="forbidden_import",
                    message=f"from {module} import is not allowed in strategy code",
                    module=module,
                    line=getattr(node, "lineno", 0),
                )
            )

    shared_safety = (
        validate_platform_import_safety(
            tree,
            policy=HOSTED_PLATFORM_IMPORT_POLICY,
        )
        + validate_dynamic_import_safety(tree)
    )
    safety_by_key = {}
    for issue in shared_safety:
        safety_by_key.setdefault(
            (issue.line, issue.module, issue.symbol, issue.code),
            issue,
        )
    safety_issues = tuple(
        sorted(
            safety_by_key.values(),
            key=lambda issue: (
                issue.line,
                issue.module,
                issue.symbol,
                issue.code,
            ),
        )
    )
    rejected_imports = {
        (issue.line, issue.module)
        for issue in safety_issues
        if issue.module
    }

    static_issues = relative_issues + [
        StrategyValidationIssue(
            code=issue.code,
            message=issue.message,
            module=issue.module,
            symbol=issue.symbol,
            line=issue.line,
        )
        for issue in safety_issues
    ]
    for issue in validate_dependency_imports(
        tree,
        profile=_DEPENDENCY_PROFILE,
        stdlib_roots=_stdlib_modules(),
        platform_modules=_HOSTED_PLATFORM_MODULES,
    ):
        if (issue.line, issue.module) in rejected_imports:
            continue
        static_issues.append(
            StrategyValidationIssue(
                code=issue.code,
                message=issue.message,
                module=issue.module,
                line=issue.line,
            )
        )
    static_by_key = {}
    for issue in static_issues:
        static_by_key.setdefault(
            (issue.line, issue.module, issue.symbol, issue.code),
            issue,
        )
    issues.extend(
        sorted(
            static_by_key.values(),
            key=lambda issue: (
                issue.line,
                issue.module,
                issue.symbol,
                issue.code,
                issue.message,
            ),
        )
    )

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


def _stdlib_modules() -> set[str]:
    modules = set(getattr(sys, "stdlib_module_names", set()))
    modules.update({"__future__", "typing"})
    return modules


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
        _validate_order_targets_literal(
            assignments["ORDER_TARGETS"],
            assignments.get("LEVERAGE"),
            issues,
        )

    if "RISK_CONTROLS" in assignments:
        _validate_risk_controls_literal(assignments["RISK_CONTROLS"], issues)

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


def _validate_order_targets_literal(
    node: ast.AST,
    strategy_leverage_node: ast.AST | None,
    issues: list[StrategyValidationIssue],
) -> None:
    strategy_leverage = None
    strategy_leverage_valid = True
    if strategy_leverage_node is not None:
        try:
            strategy_leverage = _literal_eval_phase3(strategy_leverage_node)
        except ValueError:
            strategy_leverage_valid = False
            issues.append(
                StrategyValidationIssue(
                    code="invalid_leverage",
                    message=(
                        "invalid LEVERAGE declaration: LEVERAGE must be a "
                        "literal positive integer"
                    ),
                    line=getattr(strategy_leverage_node, "lineno", 0),
                )
            )
        else:
            if strategy_leverage is None:
                strategy_leverage_valid = False
                issues.append(
                    StrategyValidationIssue(
                        code="invalid_leverage",
                        message=(
                            "invalid LEVERAGE declaration: LEVERAGE must be a "
                            "literal positive integer"
                        ),
                        line=getattr(strategy_leverage_node, "lineno", 0),
                    )
                )

    target_nodes = _order_target_entry_nodes(node)
    for target_node in target_nodes:
        target_leverage_node = _dict_value_node(target_node, "leverage")
        if target_leverage_node is None:
            continue
        try:
            target_leverage = _literal_eval_phase3(target_leverage_node)
        except ValueError:
            issues.append(
                StrategyValidationIssue(
                    code="invalid_order_targets",
                    message=(
                        "invalid ORDER_TARGETS declaration: leverage must be a "
                        "literal positive integer"
                    ),
                    line=getattr(target_node, "lineno", 0),
                )
            )
            return
        if target_leverage is None:
            issues.append(
                StrategyValidationIssue(
                    code="invalid_order_targets",
                    message=(
                        "invalid ORDER_TARGETS declaration: leverage must be a "
                        "literal positive integer"
                    ),
                    line=getattr(target_node, "lineno", 0),
                )
            )
            return

    try:
        raw = _literal_eval_phase3(node)
        order_targets = parse_order_targets(raw)
    except (ValueError, StrategyDeclarationError) as exc:
        issues.append(
            StrategyValidationIssue(
                code="invalid_order_targets",
                message=f"invalid ORDER_TARGETS declaration: {exc}",
                line=getattr(node, "lineno", 0),
            )
        )
        return

    for target_node, target in zip(target_nodes, order_targets, strict=False):
        if target.leverage is None:
            continue
        try:
            resolve_order_target_leverages([target], None)
        except StrategyDeclarationError as exc:
            issues.append(
                StrategyValidationIssue(
                    code="invalid_order_targets",
                    message=f"invalid ORDER_TARGETS declaration: {exc}",
                    line=getattr(target_node, "lineno", 0),
                )
            )
            return

    if not strategy_leverage_valid:
        return
    try:
        resolve_order_target_leverages(order_targets, strategy_leverage)
    except StrategyDeclarationError as exc:
        issues.append(
            StrategyValidationIssue(
                code="invalid_leverage",
                message=f"invalid LEVERAGE declaration: {exc}",
                line=getattr(strategy_leverage_node or node, "lineno", 0),
            )
        )


def _order_target_entry_nodes(node: ast.AST) -> list[ast.AST]:
    if isinstance(node, (ast.List, ast.Tuple)):
        return list(node.elts)
    return []


def _dict_value_node(node: ast.AST, field_name: str) -> ast.AST | None:
    if not isinstance(node, ast.Dict):
        return None
    for key_node, value_node in zip(node.keys, node.values, strict=True):
        if isinstance(key_node, ast.Constant) and key_node.value == field_name:
            return value_node
    return None


def _validate_risk_controls_literal(node: ast.AST, issues: list[StrategyValidationIssue]) -> None:
    try:
        raw = _literal_eval_phase3(node)
        parse_risk_controls(raw)
    except (ValueError, StrategyDeclarationError) as exc:
        issues.append(
            StrategyValidationIssue(
                code="invalid_risk_controls",
                message=f"invalid RISK_CONTROLS declaration: {exc}",
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
        if ok and value is not None and (
            isinstance(value, bool) or not isinstance(value, (str, int, float))
        ):
            _append_order_decision_issue(
                issues,
                node,
                f"OrderDecision {field} must be decimal-compatible",
            )
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
