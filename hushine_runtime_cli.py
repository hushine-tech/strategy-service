from __future__ import annotations

import sys
from pathlib import Path


def _prepare_paths() -> None:
    root = Path(__file__).resolve().parent
    candidates = [
        root,
        root / "strategy-library",
        root.parent / "strategy-library",
    ]
    for path in reversed(candidates):
        if path.exists():
            value = str(path)
            if value in sys.path:
                sys.path.remove(value)
            sys.path.insert(0, value)


def main(argv: list[str] | None = None) -> int:
    _prepare_paths()
    from strategy_service.cli.hushine_runtime import main as runtime_main

    return runtime_main(argv)


def debug_main(argv: list[str] | None = None) -> int:
    _prepare_paths()
    from strategy_service.cli.hushine_debug import main as debug_cli_main

    return debug_cli_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
