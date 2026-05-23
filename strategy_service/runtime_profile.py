from __future__ import annotations

from dataclasses import dataclass

RUNTIME_VERSION = "1.0.0"
RUNTIME_PROFILE = "platform-python-3.13"

ALLOWED_THIRD_PARTY_MODULES: frozenset[str] = frozenset(
    {
        "dateutil",
        "google",
        "grpc",
        "numpy",
        "pandas",
        "pydantic",
        "requests",
        "yaml",
    }
)

DEBUGGER_ONLY_MODULES: frozenset[str] = frozenset(
    {
        "debugpy",
        "pydevd",
        "pydevd_pycharm",
    }
)


@dataclass(frozen=True)
class RuntimeProfile:
    version: str
    name: str
    allowed_third_party_modules: frozenset[str]


def current_runtime_profile() -> RuntimeProfile:
    return RuntimeProfile(
        version=RUNTIME_VERSION,
        name=RUNTIME_PROFILE,
        allowed_third_party_modules=ALLOWED_THIRD_PARTY_MODULES,
    )
