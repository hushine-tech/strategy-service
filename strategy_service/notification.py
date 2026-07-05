from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class NoopNotificationClient:
    def publish(self, **kwargs) -> bool:  # noqa: ANN003
        return False


class ControlPanelNotificationClient:
    def __init__(
        self,
        stub,
        *,
        user_id: int,
        runtime_id: str,
        timeout_seconds: float = 2.0,
    ) -> None:
        self._stub = stub
        self._user_id = int(user_id or 0)
        self._runtime_id = str(runtime_id or "").strip()
        self._timeout_seconds = float(timeout_seconds or 2.0)

    def publish(
        self,
        *,
        message: str,
        severity: str = "info",
        title: str = "",
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        dedupe_key: str = "",
        category: str = "custom",
    ) -> bool:
        if self._stub is None or self._user_id <= 0 or not self._runtime_id:
            return False
        message = str(message or "").strip()
        if not message:
            return False
        try:
            from strategy_service.gen import control_panel_service_pb2 as cp_pb2

            resp = self._stub.PublishRuntimeNotification(
                cp_pb2.PublishRuntimeNotificationRequest(
                    user_id=self._user_id,
                    runtime_id=self._runtime_id,
                    session_id=str(session_id or ""),
                    portfolio_id=int(portfolio_id or 0),
                    strategy_id=int(strategy_id or 0),
                    category=str(category or "custom"),
                    severity=_normalize_severity(severity),
                    title=str(title or ""),
                    message=message,
                    dedupe_key=str(dedupe_key or ""),
                ),
                timeout=self._timeout_seconds,
            )
            return bool(getattr(resp, "accepted", False))
        except Exception:  # noqa: BLE001
            logger.warning("strategy notification publish failed", exc_info=True)
            return False


@dataclass(frozen=True)
class _NotificationContext:
    portfolio_id: int = 0
    strategy_id: int = 0
    session_id: str = ""


class StrategyNotifier:
    def __init__(self, client=None, context: _NotificationContext | None = None) -> None:
        self._client = client or NoopNotificationClient()
        self._context = context or _NotificationContext()

    def bind_context(
        self,
        *,
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
    ) -> "StrategyNotifier":
        return StrategyNotifier(
            self._client,
            _NotificationContext(
                portfolio_id=int(portfolio_id or 0),
                strategy_id=int(strategy_id or 0),
                session_id=str(session_id or ""),
            ),
        )

    def __call__(self, message: str, title: str = "", severity: str = "info", **kwargs) -> bool:  # noqa: ANN003
        return self._publish(severity, message, title, **kwargs)

    def info(self, message: str, title: str = "", **kwargs) -> bool:  # noqa: ANN003
        return self._publish("info", message, title, **kwargs)

    def warn(self, message: str, title: str = "", **kwargs) -> bool:  # noqa: ANN003
        return self._publish("warn", message, title, **kwargs)

    def error(self, message: str, title: str = "", **kwargs) -> bool:  # noqa: ANN003
        return self._publish("error", message, title, **kwargs)

    def _publish(self, severity: str, message: str, title: str = "", **kwargs) -> bool:  # noqa: ANN003
        try:
            return bool(self._client.publish(
                message=str(message or ""),
                severity=_normalize_severity(severity),
                title=str(title or ""),
                portfolio_id=int(kwargs.get("portfolio_id", self._context.portfolio_id) or 0),
                strategy_id=int(kwargs.get("strategy_id", self._context.strategy_id) or 0),
                session_id=str(kwargs.get("session_id", self._context.session_id) or ""),
                dedupe_key=str(kwargs.get("dedupe_key", "") or ""),
                category="custom",
            ))
        except Exception:  # noqa: BLE001
            logger.warning("strategy notifier swallowed publish failure", exc_info=True)
            return False


def _normalize_severity(severity: str) -> str:
    value = str(severity or "info").strip().lower()
    if value in ("warn", "warning"):
        return "warn"
    if value == "error":
        return "error"
    return "info"
