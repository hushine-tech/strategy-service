#!/usr/bin/env python3
"""把 bare 本地调试策略代码回传到远端 portfolio 数据库。

本脚本只扫描 `.hushine-runtime/strategies/user-*/strategy-*.py`，按文件名中的
`strategy_id` 更新 `strategies.code`。它不会把 `.hushine-runtime` 加入 git。
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.extensions import connection as PGConnection


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOT = ROOT / ".hushine-runtime" / "strategies"

DEFAULT_DB_HOST = os.environ.get("PGHOST", "127.0.0.1")
DEFAULT_DB_PORT = int(os.environ.get("PGPORT", "5432"))
DEFAULT_DB_NAME = os.environ.get("PGDATABASE", "portfolio")
DEFAULT_DB_USER = os.environ.get("PGUSER", "postgres")
DEFAULT_DB_PASSWORD = os.environ.get("PGPASSWORD", "postgres")

USER_DIR_RE = re.compile(r"^user-(?P<user_id>\d+)$")
STRATEGY_FILE_RE = re.compile(r"^strategy-(?P<strategy_id>\d+)-.+\.py$")


@dataclass(frozen=True)
class LocalStrategyFile:
    path: Path
    user_id: int
    strategy_id: int


@dataclass(frozen=True)
class RemoteStrategy:
    strategy_id: int
    user_id: int
    name: str
    version: str
    archived: bool
    code: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload bare debug strategy files back to the portfolio database.",
    )
    parser.add_argument(
        "--source-root",
        default=str(DEFAULT_SOURCE_ROOT),
        help=f"local debug strategy root (default: {DEFAULT_SOURCE_ROOT})",
    )
    parser.add_argument("--user-id", type=int, default=0, help="only upload one user directory")
    parser.add_argument("--strategy-id", type=int, default=0, help="only upload one strategy id")
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST, help=f"PostgreSQL host (default: {DEFAULT_DB_HOST})")
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help=f"PostgreSQL port (default: {DEFAULT_DB_PORT})")
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME, help=f"PostgreSQL database (default: {DEFAULT_DB_NAME})")
    parser.add_argument("--db-user", default=DEFAULT_DB_USER, help=f"PostgreSQL user (default: {DEFAULT_DB_USER})")
    parser.add_argument(
        "--db-password",
        default=DEFAULT_DB_PASSWORD,
        help="PostgreSQL password (default: PGPASSWORD or postgres)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print planned updates without writing to the database",
    )
    parser.add_argument(
        "--include-archived",
        action="store_true",
        help="also update archived strategies",
    )
    parser.add_argument(
        "--allow-user-mismatch",
        action="store_true",
        help="update even when file user id and database user id differ",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="skip local files whose strategy_id does not exist in the target database",
    )
    return parser


def discover_strategy_files(source_root: Path, *, user_id: int = 0, strategy_id: int = 0) -> list[LocalStrategyFile]:
    if not source_root.exists():
        raise FileNotFoundError(f"source root does not exist: {source_root}")
    if not source_root.is_dir():
        raise NotADirectoryError(f"source root is not a directory: {source_root}")

    found: list[LocalStrategyFile] = []
    for user_dir in sorted(source_root.iterdir()):
        if not user_dir.is_dir():
            continue
        user_match = USER_DIR_RE.match(user_dir.name)
        if not user_match:
            continue
        file_user_id = int(user_match.group("user_id"))
        if user_id > 0 and file_user_id != user_id:
            continue
        for path in sorted(user_dir.glob("strategy-*.py")):
            file_match = STRATEGY_FILE_RE.match(path.name)
            if not file_match:
                continue
            file_strategy_id = int(file_match.group("strategy_id"))
            if strategy_id > 0 and file_strategy_id != strategy_id:
                continue
            found.append(
                LocalStrategyFile(
                    path=path,
                    user_id=file_user_id,
                    strategy_id=file_strategy_id,
                )
            )
    return found


def connect(args: argparse.Namespace) -> PGConnection:
    dsn = (
        f"host={args.db_host} port={args.db_port} dbname={args.db_name} "
        f"user={args.db_user} password={args.db_password} sslmode=disable"
    )
    return psycopg2.connect(dsn)


def fetch_remote_strategy(conn: PGConnection, strategy_id: int) -> RemoteStrategy | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT strategy_id, user_id, name, version, archived, code
            FROM strategies
            WHERE strategy_id = %s
            """,
            (strategy_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return RemoteStrategy(
        strategy_id=int(row[0]),
        user_id=int(row[1]),
        name=str(row[2]),
        version=str(row[3]),
        archived=bool(row[4]),
        code=str(row[5]),
    )


def update_remote_strategy(conn: PGConnection, *, strategy_id: int, user_id: int, code: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE strategies
            SET code = %s
            WHERE strategy_id = %s AND user_id = %s
            """,
            (code, strategy_id, user_id),
        )
        return int(cur.rowcount)


def upload_one(conn: PGConnection, item: LocalStrategyFile, args: argparse.Namespace) -> str:
    remote = fetch_remote_strategy(conn, item.strategy_id)
    if remote is None:
        if args.skip_missing:
            return f"skip missing strategy_id={item.strategy_id} path={item.path}"
        raise RuntimeError(f"{item.path}: remote strategy_id={item.strategy_id} does not exist")
    if remote.user_id != item.user_id and not args.allow_user_mismatch:
        raise RuntimeError(
            f"{item.path}: user mismatch, file user_id={item.user_id}, "
            f"database user_id={remote.user_id}"
        )
    if remote.archived and not args.include_archived:
        return f"skip archived strategy_id={item.strategy_id} name={remote.name!r} path={item.path}"

    code = item.path.read_text(encoding="utf-8")
    if code == remote.code:
        return f"unchanged strategy_id={item.strategy_id} name={remote.name!r} path={item.path}"
    if args.dry_run:
        return (
            f"would update strategy_id={item.strategy_id} name={remote.name!r} "
            f"version={remote.version!r} bytes={len(code)} path={item.path}"
        )

    matched = update_remote_strategy(
        conn,
        strategy_id=item.strategy_id,
        user_id=remote.user_id,
        code=code,
    )
    if matched != 1:
        raise RuntimeError(f"{item.path}: update matched {matched} rows")
    return (
        f"updated strategy_id={item.strategy_id} name={remote.name!r} "
        f"version={remote.version!r} bytes={len(code)} path={item.path}"
    )


def iter_results(conn: PGConnection, items: Iterable[LocalStrategyFile], args: argparse.Namespace) -> tuple[int, int]:
    changed = 0
    errors = 0
    for item in items:
        try:
            result = upload_one(conn, item, args)
            print(result)
            if result.startswith("updated ") or result.startswith("would update "):
                changed += 1
        except Exception as exc:  # noqa: BLE001
            errors += 1
            print(f"error: {exc}", file=sys.stderr)
    return changed, errors


def main() -> int:
    args = build_parser().parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    try:
        items = discover_strategy_files(source_root, user_id=args.user_id, strategy_id=args.strategy_id)
    except Exception as exc:  # noqa: BLE001
        print(f"failed to scan strategies: {exc}", file=sys.stderr)
        return 1
    if not items:
        print(f"no strategy files found under {source_root}")
        return 0

    print(f"source root: {source_root}")
    print(f"database: {args.db_host}:{args.db_port}/{args.db_name}")
    print(f"files: {len(items)}")
    if args.dry_run:
        print("mode: dry-run")

    try:
        with connect(args) as conn:
            changed, errors = iter_results(conn, items, args)
            if args.dry_run:
                conn.rollback()
            elif errors:
                conn.rollback()
            else:
                conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"upload failed: {exc}", file=sys.stderr)
        return 1
    print(f"summary: changed={changed} errors={errors}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
