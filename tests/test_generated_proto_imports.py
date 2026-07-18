from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


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
