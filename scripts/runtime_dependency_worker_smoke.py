#!/usr/bin/env python3
"""Validate the image dependency closure and exercise a real one-shot worker."""

from __future__ import annotations

import argparse
from concurrent import futures
from importlib import metadata
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import uuid

import grpc
from google.protobuf.any_pb2 import Any as ProtoAny

from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile
from hushine_strategy.validator import validate_strategy_code as validate_sdk_strategy
from strategy_service.gen import control_panel_service_pb2
from strategy_service.gen import runtime_worker_pb2 as worker_pb2
from strategy_service.gen import runtime_worker_pb2_grpc as worker_grpc
from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.strategy_imports import gate_strategy_source, resolve_strategy_source
from strategy_service.strategy_validator import validate_strategy_code


def representative_strategy_source(body: str) -> str:
    profile = load_runtime_dependency_profile()
    imports = "\n".join(
        f"import {dependency.probe}"
        for dependency in profile.dependencies
        if dependency.public
    )
    return f"{imports}\n{body}"


def _require_profile(expected_profile: str, expected_version: str, expected_digest: str):
    profile = load_runtime_dependency_profile()
    actual = (profile.profile_name, profile.profile_version, profile.contract_sha256)
    expected = (expected_profile, expected_version, expected_digest)
    if actual != expected:
        raise RuntimeError("packaged runtime dependency profile does not match expected facts")
    source_manifest = Path(
        "/app/strategy-library/hushine_strategy/runtime_dependencies.toml"
    )
    if source_manifest.exists():
        import hashlib

        if hashlib.sha256(source_manifest.read_bytes()).hexdigest() != expected_digest:
            raise RuntimeError("installed package manifest does not match sealed source digest")
    return profile


def _require_coverage_mode(coverage: bool, body: str) -> None:
    try:
        metadata.version("coverage")
        installed = True
    except metadata.PackageNotFoundError:
        installed = False
    if installed is not coverage:
        state = "present" if installed else "absent"
        raise RuntimeError(f"coverage distribution is unexpectedly {state}")
    denied = validate_strategy_code(f"import coverage\n{body}")
    if denied.ok or not any(
        issue.code == "UNSUPPORTED_STRATEGY_DEPENDENCY" for issue in denied.issues
    ):
        raise RuntimeError("coverage must remain unavailable to user strategies")


def validate_representative_strategy(
    *,
    body_path: Path,
    expected_profile: str,
    expected_version: str,
    expected_digest: str,
    coverage: bool,
) -> str:
    _require_profile(expected_profile, expected_version, expected_digest)
    body = body_path.read_text(encoding="utf-8")
    source = representative_strategy_source(body)
    hosted = validate_strategy_code(source)
    sdk = validate_sdk_strategy(source)
    if not hosted.ok or not sdk.ok:
        raise RuntimeError("representative strategy failed static dependency validation")
    resolved = resolve_strategy_source("<runtime-dependency-smoke>", source)
    gate = gate_strategy_source(
        resolved,
        python_invocation_path=os.path.abspath(os.path.normpath(sys.executable)),
    )
    if not gate.ok or gate.issues or gate.dependency_error is not None:
        raise RuntimeError("representative strategy failed complete-path import validation")
    _require_coverage_mode(coverage, body)
    return source


class _WorkerSmokeServer(worker_grpc.RuntimeWorkerAgentServicer):
    def __init__(
        self,
        *,
        token: str,
        session_id: str,
        source: str,
        expected_profile: str,
        expected_version: str,
        expected_digest: str,
    ) -> None:
        self.token = token
        self.session_id = session_id
        self.source = source
        self.expected_profile = expected_profile
        self.expected_version = expected_version
        self.expected_digest = expected_digest
        self.finished = threading.Event()
        self.failure: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)

    def Connect(self, request_iterator, context):  # noqa: N802
        try:
            iterator = iter(request_iterator)
            hello_frame = next(iterator)
            if hello_frame.WhichOneof("payload") != "hello":
                raise RuntimeError("worker did not send WorkerHello first")
            hello = hello_frame.hello
            if hello.session_id != self.session_id or hello.token != self.token:
                raise RuntimeError("worker hello identity mismatch")
            if hello.pid <= 0 or not hello.worker_version:
                raise RuntimeError("worker hello metadata is incomplete")

            request = strategy_pb2.ValidateStrategySourceRequest(
                source=self.source,
                user_id=1,
                runtime_id="runtime-dependency-smoke",
            )
            packed = ProtoAny()
            packed.Pack(request)
            call_id = uuid.uuid4().hex
            yield worker_pb2.AgentFrame(
                frame_id=uuid.uuid4().hex,
                platform_call=worker_pb2.PlatformCall(
                    call_id=call_id,
                    method="ValidateStrategySource",
                    request=packed,
                    timeout_ms=25_000,
                ),
            )

            result_frame = next(iterator)
            if result_frame.WhichOneof("payload") != "platform_call_result":
                raise RuntimeError("worker did not return PlatformCallResult")
            result = result_frame.platform_call_result
            if result.call_id != call_id or not result.ok or result.error:
                raise RuntimeError("worker ValidateStrategySource call failed")
            response = strategy_pb2.ValidateStrategySourceResponse()
            if not result.response.Unpack(response) or not response.ok or response.issues:
                raise RuntimeError("worker rejected representative strategy")
            profile = response.runtime_profile
            if (
                profile.profile_name != self.expected_profile
                or profile.profile_version != self.expected_version
                or profile.contract_sha256 != self.expected_digest
            ):
                raise RuntimeError("worker returned unexpected runtime dependency profile")
            self.failure.put(None)
        except BaseException as error:  # noqa: BLE001
            self.failure.put(error)
            context.abort(grpc.StatusCode.INTERNAL, "runtime dependency worker smoke failed")
        finally:
            self.finished.set()


def run_worker_smoke(
    *,
    source: str,
    expected_profile: str,
    expected_version: str,
    expected_digest: str,
    timeout_seconds: float = 30.0,
) -> None:
    token = uuid.uuid4().hex
    session_id = uuid.uuid4().hex
    servicer = _WorkerSmokeServer(
        token=token,
        session_id=session_id,
        source=source,
        expected_profile=expected_profile,
        expected_version=expected_version,
        expected_digest=expected_digest,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    worker_grpc.add_RuntimeWorkerAgentServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    if port <= 0:
        raise RuntimeError("could not bind loopback worker smoke server")
    server.start()
    environment = os.environ.copy()
    environment.update(
        {
            "HUSHINE_AGENT_ADDR": f"127.0.0.1:{port}",
            "HUSHINE_SESSION_ID": session_id,
            "HUSHINE_WORKER_TOKEN": token,
            "HUSHINE_RUNTIME_ID": "runtime-dependency-smoke",
            "HUSHINE_RUNTIME_SOURCE": "hosted",
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "strategy_service.session_worker_entry"],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        close_fds=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        raise RuntimeError("one-shot session worker did not exit within 30 seconds") from None
    finally:
        server.stop(grace=0).wait(timeout=5)
    if process.returncode != 0:
        detail = " ".join((stderr or stdout).split())[-1000:]
        raise RuntimeError(f"one-shot session worker exited {process.returncode}: {detail}")
    if not servicer.finished.wait(timeout=2.0):
        raise RuntimeError("worker smoke server did not observe a result")
    failure = servicer.failure.get_nowait()
    if failure is not None:
        raise RuntimeError(str(failure)) from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="runtime_dependency_worker_smoke.py")
    parser.add_argument("--strategy-body", type=Path, required=True)
    parser.add_argument("--expected-profile", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-digest", required=True)
    parser.add_argument("--coverage", choices=("true", "false"), required=True)
    parser.add_argument("--check-only", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    options = _parser().parse_args(argv)
    try:
        source = validate_representative_strategy(
            body_path=options.strategy_body,
            expected_profile=options.expected_profile,
            expected_version=options.expected_version,
            expected_digest=options.expected_digest,
            coverage=options.coverage == "true",
        )
        if not options.check_only:
            run_worker_smoke(
                source=source,
                expected_profile=options.expected_profile,
                expected_version=options.expected_version,
                expected_digest=options.expected_digest,
            )
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "contract_sha256": options.expected_digest,
                "coverage": options.coverage == "true",
                "ok": True,
                "profile_name": options.expected_profile,
                "profile_version": options.expected_version,
                "worker": not options.check_only,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
