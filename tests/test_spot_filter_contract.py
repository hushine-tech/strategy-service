from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import urllib.request

import pytest

from strategy_service.wallet.spot_filters import evaluate_spot_filter_vector


FIXTURE = Path(__file__).with_name("fixtures") / "spot_filter_contract_v1.json"
CORE_FIXTURE = (
    Path(__file__).parents[2]
    / "core-service"
    / "internal"
    / "order"
    / "risk"
    / "testdata"
    / "spot_filter_contract_v1.json"
)


def _document() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_hosted_filter_fixture_is_byte_identical_to_core_contract():
    assert hashlib.sha256(FIXTURE.read_bytes()).digest() == hashlib.sha256(
        CORE_FIXTURE.read_bytes()
    ).digest()


@pytest.mark.parametrize("vector", _document()["cases"], ids=lambda item: item["name"])
def test_hosted_spot_filter_contract_has_core_code_and_no_network(monkeypatch, vector):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Spot filter evaluation must not use networking")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)

    assert evaluate_spot_filter_vector(vector) == vector["expected_code"]
