from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

from google.protobuf.descriptor import FieldDescriptor
import pytest

from strategy_service.gen import order_service_pb2 as order_pb2
from strategy_service.gen import portfolio_service_pb2 as portfolio_pb2
from strategy_service.gen import strategy_service_pb2 as strategy_pb2


def test_order_proto_imports_from_outside_repository(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repository)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from strategy_service.gen import order_service_pb2; "
                "assert order_service_pb2.CloseSpotTargetsRequest.DESCRIPTOR.full_name "
                "== 'order.v1.CloseSpotTargetsRequest'"
            ),
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_spot_wallet_uses_only_canonical_assets_and_exact_balances():
    wallet = portfolio_pb2.SpotWallet.DESCRIPTOR.fields_by_name
    asset = portfolio_pb2.SpotAsset.DESCRIPTOR.fields_by_name

    assert {"assets"} <= set(wallet)
    assert {"asset", "free_decimal", "locked_decimal"} <= set(asset)

    forbidden = {
        "SpotWallet": {"free", "locked"} & set(wallet),
        "SpotAsset": {"symbol", "qty", "free", "locked"} & set(asset),
    }
    assert forbidden == {"SpotWallet": set(), "SpotAsset": set()}


@pytest.mark.parametrize(
    ("contract_name", "descriptor", "legacy_fields", "exact_fields"),
    [
        (
            "PlaceOrderRequest",
            order_pb2.PlaceOrderRequest.DESCRIPTOR,
            {"qty", "price", "mark_price"},
            {"qty_decimal", "price_decimal", "mark_price_decimal"},
        ),
        (
            "OrderIntentEntry",
            order_pb2.OrderIntentEntry.DESCRIPTOR,
            {"requested_qty", "requested_price"},
            {"requested_qty_decimal", "requested_price_decimal"},
        ),
        (
            "OrderAttemptEntry",
            order_pb2.OrderAttemptEntry.DESCRIPTOR,
            {"requested_qty", "requested_price", "mark_price"},
            {
                "requested_qty_decimal",
                "requested_price_decimal",
                "mark_price_decimal",
            },
        ),
        (
            "OrderEntry",
            order_pb2.ExchangeOrderEntry.DESCRIPTOR,
            {"orig_qty", "executed_qty", "remaining_qty", "avg_price", "price"},
            {
                "orig_qty_decimal",
                "executed_qty_decimal",
                "remaining_qty_decimal",
                "avg_price_decimal",
                "price_decimal",
                "cumulative_quote_qty_decimal",
            },
        ),
        (
            "FillEntry",
            order_pb2.OrderFillEntry.DESCRIPTOR,
            {"qty", "fill_price", "fee"},
            {
                "qty_decimal",
                "fill_price_decimal",
                "fee_decimal",
                "quote_qty_decimal",
            },
        ),
        (
            "FillDelta",
            order_pb2.FillDeltaEntry.DESCRIPTOR,
            {"qty", "fill_price", "fee"},
            {
                "qty_decimal",
                "fill_price_decimal",
                "fee_decimal",
                "quote_qty_decimal",
            },
        ),
        (
            "OrderStateDelta",
            order_pb2.OrderStateEntry.DESCRIPTOR,
            {"orig_qty", "executed_qty", "remaining_qty", "avg_price"},
            {
                "orig_qty_decimal",
                "executed_qty_decimal",
                "remaining_qty_decimal",
                "avg_price_decimal",
                "price_decimal",
                "cumulative_quote_qty_decimal",
            },
        ),
    ],
)
def test_order_business_values_use_exact_decimal_fields_only(
    contract_name, descriptor, legacy_fields, exact_fields
):
    fields = descriptor.fields_by_name

    assert exact_fields <= set(fields), f"{contract_name} lost exact decimal fields"

    present_legacy_fields = legacy_fields & set(fields)
    present_legacy_doubles = {
        name
        for name in present_legacy_fields
        if fields[name].type == FieldDescriptor.TYPE_DOUBLE
    }
    assert not present_legacy_doubles, (
        f"{contract_name} still exposes parallel double business fields: "
        f"{sorted(present_legacy_doubles)}"
    )


def _field(message_name: str, field_name: str):
    message = portfolio_pb2.DESCRIPTOR.message_types_by_name.get(message_name)
    assert message is not None, f"portfolio.v1.{message_name} is missing"
    field = message.fields_by_name.get(field_name)
    assert field is not None, f"{message.full_name}.{field_name} is missing"
    return field


def test_session_wide_leverage_fields_do_not_exist():
    assert "leverage" not in strategy_pb2.RunStrategyRequest.DESCRIPTOR.fields_by_name
    assert (
        "leverage"
        not in strategy_pb2.PreviewRunStrategyRequest.DESCRIPTOR.fields_by_name
    )
    assert "leverage" not in strategy_pb2.RiskControls.DESCRIPTOR.fields_by_name
    assert "leverage_source" not in strategy_pb2.RiskControls.DESCRIPTOR.fields_by_name
    assert (
        "leverage"
        not in portfolio_pb2.PreflightStrategySessionRequest.DESCRIPTOR.fields_by_name
    )
    assert "leverage" not in portfolio_pb2.StrategySessionEntry.DESCRIPTOR.fields_by_name
    assert "leverage" not in portfolio_pb2.SaveSessionRequest.DESCRIPTOR.fields_by_name


def test_portfolio_leverage_preview_contract_is_additive_and_read_only():
    required_symbol = portfolio_pb2.RequiredSymbol.DESCRIPTOR.fields_by_name
    assert required_symbol["exchange"].number == 1
    assert required_symbol["market"].number == 2
    assert required_symbol["symbol"].number == 3
    assert required_symbol["order_target"].number == 4
    assert required_symbol["required_order_types"].number == 5
    assert required_symbol["effective_leverage"].number == 6
    assert required_symbol["effective_leverage"].type == FieldDescriptor.TYPE_UINT32
    assert required_symbol["leverage_source"].number == 7

    expected_preview_fields = {
        "venue_id": 1,
        "exchange": 2,
        "market": 3,
        "symbol": 4,
        "effective_leverage": 5,
        "leverage_source": 6,
        "current_leverage": 7,
        "change_required": 8,
        "status": 9,
        "error_code": 10,
        "error_message": 11,
        "retryable": 12,
    }
    preview = portfolio_pb2.FuturesLeveragePreview.DESCRIPTOR
    assert {field.name: field.number for field in preview.fields} == expected_preview_fields
    assert preview.fields_by_name["current_leverage"].has_presence
    assert preview.fields_by_name["current_leverage"].type == FieldDescriptor.TYPE_UINT32

    previews = _field(
        "PreflightStrategySessionResponse", "futures_leverage_previews"
    )
    assert previews.number == 5
    assert previews.is_repeated
    assert previews.message_type.full_name == "portfolio.v1.FuturesLeveragePreview"

    request = portfolio_pb2.PreflightStrategySessionRequest.DESCRIPTOR
    assert "apply" not in request.fields_by_name


def test_portfolio_commit_and_session_target_fact_contract_is_additive():
    service = portfolio_pb2.DESCRIPTOR.services_by_name["PortfolioService"]
    method = service.methods_by_name.get("CommitStrategySessionStart")
    assert method is not None
    assert method.input_type.full_name == "portfolio.v1.CommitStrategySessionStartRequest"
    assert method.output_type.full_name == "portfolio.v1.CommitStrategySessionStartResponse"

    request = portfolio_pb2.CommitStrategySessionStartRequest.DESCRIPTOR
    assert {field.name: field.number for field in request.fields} == {
        "launch_operation_id": 1,
        "session": 2,
        "required_routes": 3,
        "required_symbols": 4,
        "resume_session_id": 5,
    }
    assert request.fields_by_name["session"].message_type.full_name == (
        "portfolio.v1.SaveSessionRequest"
    )
    assert request.fields_by_name["required_routes"].is_repeated
    assert request.fields_by_name["required_symbols"].is_repeated

    response = portfolio_pb2.CommitStrategySessionStartResponse.DESCRIPTOR
    assert {field.name: field.number for field in response.fields} == {
        "ok": 1,
        "issues": 2,
        "confirmed_target_facts": 3,
        "target_results": 4,
        "rollback_failed": 5,
        "code": 6,
    }
    assert response.fields_by_name["issues"].is_repeated
    assert response.fields_by_name["confirmed_target_facts"].is_repeated
    assert response.fields_by_name[
        "confirmed_target_facts"
    ].message_type.full_name == "portfolio.v1.SessionTargetLeverageFact"
    assert response.fields_by_name["target_results"].is_repeated
    assert response.fields_by_name["target_results"].message_type.full_name == (
        "portfolio.v1.FuturesLeverageTargetResult"
    )

    session = portfolio_pb2.StrategySessionEntry.DESCRIPTOR
    assert session.fields_by_name["launch_operation_id"].number == 27

    update = portfolio_pb2.UpdateSessionRequest.DESCRIPTOR
    assert update.fields_by_name["expected_status"].number == 7

    result = portfolio_pb2.FuturesLeverageTargetResult.DESCRIPTOR
    assert {field.name: field.number for field in result.fields} == {
        "venue_id": 1,
        "exchange": 2,
        "market": 3,
        "symbol": 4,
        "effective_leverage": 5,
        "leverage_source": 6,
        "previous_leverage": 7,
        "current_leverage": 8,
        "confirmed_leverage": 9,
        "change_required": 10,
        "status": 11,
        "error_code": 12,
        "error_message": 13,
        "retryable": 14,
    }
    for name in ("previous_leverage", "current_leverage", "confirmed_leverage"):
        assert result.fields_by_name[name].has_presence
        assert result.fields_by_name[name].type == FieldDescriptor.TYPE_UINT32

    fact = portfolio_pb2.SessionTargetLeverageFact.DESCRIPTOR
    assert {field.name: field.number for field in fact.fields} == {
        "session_id": 1,
        "venue_id": 2,
        "exchange": 3,
        "environment": 4,
        "market": 5,
        "symbol": 6,
        "effective_leverage": 7,
        "leverage_source": 8,
        "previous_leverage": 9,
        "confirmed_leverage": 10,
        "confirmed_at": 11,
        "created_at": 12,
    }
    assert fact.fields_by_name["previous_leverage"].has_presence
    for name in ("effective_leverage", "previous_leverage", "confirmed_leverage"):
        assert fact.fields_by_name[name].type == FieldDescriptor.TYPE_UINT32

    session = portfolio_pb2.StrategySessionEntry.DESCRIPTOR
    target_facts = session.fields_by_name["target_leverage_facts"]
    assert target_facts.number == 26
    assert target_facts.is_repeated
    assert target_facts.message_type.full_name == (
        "portfolio.v1.SessionTargetLeverageFact"
    )
