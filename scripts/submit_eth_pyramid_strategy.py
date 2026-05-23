#!/usr/bin/env python3
"""Create/mount/activate the Phase C3 ETH pyramid strategy via quant-handler.

This helper exists for the real ``mode=2`` reconciliation smoke path. It uses
the same portal HTTP chain as the product UI:

    quant-handler -> account-service -> DB

Typical usage:

    python scripts/submit_eth_pyramid_strategy.py \
        --username <user> --password <pass>

    python scripts/submit_eth_pyramid_strategy.py \
        --username <user> --password <pass> \
        --account-id 13 --activate

When the same user already has the same ``name`` + ``version`` strategy, the
script reuses it instead of creating a duplicate.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib import error, parse, request


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HANDLER_URL = os.environ.get("QUANT_HANDLER_URL", "http://127.0.0.1:8090")
DEFAULT_USERNAME = os.environ.get("HUSHINE_USERNAME", "")
DEFAULT_PASSWORD = os.environ.get("HUSHINE_PASSWORD", "")
DEFAULT_NAME = os.environ.get("HUSHINE_C3_STRATEGY_NAME", "eth-pyramid-futures")
DEFAULT_VERSION = os.environ.get("HUSHINE_C3_STRATEGY_VERSION", "1.0.0")
DEFAULT_DESCRIPTION = os.environ.get(
    "HUSHINE_C3_STRATEGY_DESCRIPTION",
    "Phase C3 mode=2 ETHUSDT futures reconciliation smoke strategy",
)
DEFAULT_CODE_PATH = ROOT / "strategy_templates" / "eth_pyramid_futures.py"


class APIError(RuntimeError):
    """Raised when a handler API call fails."""


def _json_request(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    token: str | None = None,
    timeout: float = 10.0,
) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.getcode(), json.loads(raw) if raw else None
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"error": raw or exc.reason}
        return exc.code, body


def _must_succeed(status: int, body: Any, *, action: str, expected: tuple[int, ...]) -> Any:
    if status in expected:
        return body
    if isinstance(body, dict) and body.get("error"):
        raise APIError(f"{action} failed ({status}): {body['error']}")
    raise APIError(f"{action} failed ({status}): {body!r}")


def load_strategy_code(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise APIError(f"failed to read strategy code from {path}: {exc}") from exc


def login(handler_url: str, username: str, password: str) -> tuple[str, int]:
    body = {"username": username, "password": password}
    status, payload = _json_request("POST", f"{handler_url}/api/auth/login", payload=body)
    data = _must_succeed(status, payload, action="login", expected=(200,))
    token = str((data or {}).get("token") or "").strip()
    user = (data or {}).get("user") or {}
    user_id = int(user.get("id") or 0)
    if not token or user_id <= 0:
        raise APIError("login succeeded but token/user_id is missing")
    return token, user_id


def list_strategies(handler_url: str, token: str, name_prefix: str) -> list[dict[str, Any]]:
    query = parse.urlencode({"name_prefix": name_prefix})
    status, payload = _json_request(
        "GET",
        f"{handler_url}/api/strategies?{query}",
        token=token,
    )
    data = _must_succeed(status, payload, action="list strategies", expected=(200,))
    if not isinstance(data, list):
        raise APIError(f"list strategies returned unexpected payload: {data!r}")
    return data


def find_exact_strategy(
    strategies: list[dict[str, Any]],
    *,
    name: str,
    version: str,
) -> dict[str, Any] | None:
    for item in strategies:
        if item.get("name") == name and item.get("version") == version:
            return item
    return None


def create_or_reuse_strategy(
    handler_url: str,
    token: str,
    *,
    name: str,
    version: str,
    description: str,
    code: str,
) -> tuple[dict[str, Any], bool]:
    existing = find_exact_strategy(list_strategies(handler_url, token, name), name=name, version=version)
    if existing is not None:
        return existing, False

    payload = {
        "name": name,
        "version": version,
        "description": description,
        "code": code,
    }
    status, body = _json_request("POST", f"{handler_url}/api/strategies", payload=payload, token=token)
    if status == 201:
        if not isinstance(body, dict) or not body.get("strategy_id"):
            raise APIError(f"create strategy returned unexpected payload: {body!r}")
        return body, True
    if status == 409:
        existing = find_exact_strategy(
            list_strategies(handler_url, token, name),
            name=name,
            version=version,
        )
        if existing is not None:
            return existing, False
        err = body.get("error") if isinstance(body, dict) else body
        raise APIError(
            "create strategy hit a duplicate name/version that is not visible "
            f"to the current user: {err}. Pass --name/--version to avoid the collision."
        )
    _must_succeed(status, body, action="create strategy", expected=(201,))
    raise AssertionError("unreachable")


def mount_strategy(handler_url: str, token: str, *, account_id: int, strategy_id: int) -> None:
    status, body = _json_request(
        "POST",
        f"{handler_url}/api/accounts/{account_id}/strategies/{strategy_id}",
        token=token,
    )
    _must_succeed(status, body, action="mount strategy", expected=(200,))


def activate_strategy(handler_url: str, token: str, *, account_id: int, strategy_id: int) -> None:
    status, body = _json_request(
        "POST",
        f"{handler_url}/api/accounts/{account_id}/strategies/{strategy_id}/activate",
        token=token,
    )
    _must_succeed(status, body, action="activate strategy", expected=(200,))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit the Phase C3 ETH pyramid strategy through quant-handler.",
    )
    parser.add_argument(
        "--handler-url",
        default=DEFAULT_HANDLER_URL,
        help=f"quant-handler base URL (default: {DEFAULT_HANDLER_URL})",
    )
    parser.add_argument(
        "--username",
        default=DEFAULT_USERNAME,
        help="portal username (or set HUSHINE_USERNAME)",
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_PASSWORD,
        help="portal password (or set HUSHINE_PASSWORD)",
    )
    parser.add_argument(
        "--name",
        default=DEFAULT_NAME,
        help=f"strategy name (default: {DEFAULT_NAME})",
    )
    parser.add_argument(
        "--version",
        default=DEFAULT_VERSION,
        help=f"strategy version (default: {DEFAULT_VERSION})",
    )
    parser.add_argument(
        "--description",
        default=DEFAULT_DESCRIPTION,
        help="strategy description stored in the portal",
    )
    parser.add_argument(
        "--code-path",
        type=Path,
        default=DEFAULT_CODE_PATH,
        help=f"path to the strategy source file (default: {DEFAULT_CODE_PATH})",
    )
    parser.add_argument(
        "--account-id",
        type=int,
        default=0,
        help="optional account_id to mount the strategy onto",
    )
    parser.add_argument(
        "--activate",
        action="store_true",
        help="after mounting, mark the strategy active on the given account",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.username:
        print("error: --username is required (or set HUSHINE_USERNAME)", file=sys.stderr)
        return 2
    if not args.password:
        print("error: --password is required (or set HUSHINE_PASSWORD)", file=sys.stderr)
        return 2
    if args.activate and args.account_id <= 0:
        print("error: --activate requires --account-id", file=sys.stderr)
        return 2

    code = load_strategy_code(args.code_path)

    try:
        token, user_id = login(args.handler_url.rstrip("/"), args.username, args.password)
        strategy, created = create_or_reuse_strategy(
            args.handler_url.rstrip("/"),
            token,
            name=args.name,
            version=args.version,
            description=args.description,
            code=code,
        )
        strategy_id = int(strategy.get("strategy_id") or 0)
        if strategy_id <= 0:
            raise APIError(f"strategy response missing strategy_id: {strategy!r}")

        print(f"user_id={user_id}")
        print(f"strategy_id={strategy_id}")
        print(f"strategy_name={strategy.get('name')}")
        print(f"strategy_version={strategy.get('version')}")
        print(f"create_action={'created' if created else 'reused'}")

        if args.account_id > 0:
            mount_strategy(
                args.handler_url.rstrip("/"),
                token,
                account_id=args.account_id,
                strategy_id=strategy_id,
            )
            print(f"mounted_account_id={args.account_id}")
            if args.activate:
                activate_strategy(
                    args.handler_url.rstrip("/"),
                    token,
                    account_id=args.account_id,
                    strategy_id=strategy_id,
                )
                print(f"activated_account_id={args.account_id}")
                print("next_run=")
                print(
                    "curl -s -X POST "
                    f"{args.handler_url.rstrip('/')}/api/accounts/{args.account_id}/run-strategy "
                    "-H 'Authorization: Bearer <TOKEN>' "
                    "-H 'Content-Type: application/json' "
                    "-d '{\"strategy_path\":\"\",\"interval\":\"1m\"}'"
                )
        else:
            print("account_action=none")
    except APIError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
