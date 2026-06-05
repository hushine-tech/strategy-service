"""HTTP API: assemble wallet, run ``StrategyEngine`` + ``BacktestDataLoop`` (DB klines)."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Optional

from pydantic import BaseModel, Field

from market_data.config import TimescaleConfig

from strategy_service.data_loop import BacktestDataLoop
from strategy_service.order_client import OrderClient
from strategy_service.service import StrategyEngine
from strategy_service.types import Exchange, Market
from strategy_service.wallet_factory import build_wallet_from_account
from strategy_service.wallet.portfolio import PortfolioWalletRuntime

logger = logging.getLogger(__name__)


class BacktestRunResponse(BaseModel):
    ok: bool
    bars_processed: int = 0
    message: str = ""
    error: str | None = None


def _run_backtest_simple(
    loop: BacktestDataLoop,
    *,
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
    interval: str,
    market: str,
) -> int:
    """Run backtest and return bar count by wrapping the data source."""
    from strategy_service.data_loop import _adapt_kline
    from market_data.backtest import BacktestDataSource

    n = 0
    config = loop.config
    service = loop.service
    with BacktestDataSource(config) as ds:
        for kline in ds.get_klines(symbol, interval, start_time_ms, end_time_ms, market=market):
            n += 1
            service.running_strategy(_adapt_kline(kline, market))
    return n


class FuturesPositionSpec(BaseModel):
    symbol: str
    direction: int = 0
    initial_balance: float
    leverage: float
    fee_rate: float = 0.0004
    margin_mode: str | None = None


class FuturesAccountSpec(BaseModel):
    margin_mode: str = "isolated"
    position_mode: str = "one_way"
    initial_balance: float = 0.0
    deposit_sum: float = 0.0
    withdrawal_sum: float = 0.0
    positions: list[FuturesPositionSpec] = Field(default_factory=list)


class SpotAssetSpec(BaseModel):
    qty: float = 0.0
    locked: float = 0.0
    avg_entry_price: float = 0.0
    price: float | None = None


class SpotAccountSpec(BaseModel):
    free: float = 0.0
    locked: float = 0.0
    assets: dict[str, SpotAssetSpec] = Field(default_factory=dict)


class AccountSpec(BaseModel):
    futures: FuturesAccountSpec
    spot: SpotAccountSpec = Field(default_factory=SpotAccountSpec)


class TimescaleSpec(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "market_data"
    user: str = "postgres"
    password: str = ""
    pool_size: int = 5
    max_overflow: int = 10


class BacktestRunRequest(BaseModel):
    user_id: str
    strategy_path: str
    symbol: str
    symbol_type: str = "perpetual_futures"
    market: str = "perpetual_futures"
    interval: str = "1m"
    start_time_ms: int
    end_time_ms: int
    account: AccountSpec
    timescale: Optional[TimescaleSpec] = None
    account_id: int = 0
    account_service_address: str = ""
    order_service_address: str = ""


def _init_tracer() -> None:
    """初始化 OpenTelemetry，忽略未安装或路径未设置的情况。"""
    try:
        from utils.log.tracer import init_tracer  # type: ignore[import]
        service_name = os.environ.get("OTEL_SERVICE_NAME", "strategy-service")
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
        init_tracer(service_name, endpoint)
    except Exception:
        pass


def _init_logger() -> Any:
    """初始化 strategy-library 全局日志系统，输出目录从 LOG_OUTPUT_DIR 环境变量读取。"""
    try:
        from utils.log import init_log
        return init_log()
    except Exception:
        return None


def _phase3_market(market: str) -> str:
    market_key = str(market or "").strip().lower()
    if market_key in ("", "futures", "future", "usdm_futures", "perp"):
        return Market.PERPETUAL_FUTURES
    return market_key


def create_app() -> Any:
    _init_tracer()
    _log = _init_logger()

    try:
        from fastapi import FastAPI, HTTPException
    except ImportError as e:
        raise ImportError(
            "HTTP server requires 'fastapi'. Install with: pip install fastapi uvicorn"
        ) from e

    @asynccontextmanager
    async def lifespan(app):
        yield
        try:
            from utils.log import close
            close()
        except Exception:
            pass

    app = FastAPI(title="strategy-service", version="1.0.0", lifespan=lifespan)

    # 挂载 ASGI 中间件：access 日志 + session_id 传播 + panic recovery
    try:
        from utils.middleware.http.server import ASGIMiddleware
        app.add_middleware(ASGIMiddleware, logger=_log)
    except Exception:
        pass

    @app.post("/run-backtest", response_model=BacktestRunResponse)
    def run_backtest(req: BacktestRunRequest) -> BacktestRunResponse:
        try:
            wallet = build_wallet_from_account(req.account.model_dump())
            market = _phase3_market(req.market)
            symbol_type = _phase3_market(req.symbol_type)
            if symbol_type == Market.PERPETUAL_FUTURES and not wallet.futures.positions:
                raise ValueError("account.futures.positions must define at least one futures slot")

            order_client = OrderClient(req.order_service_address)

            engine = StrategyEngine()
            portfolio_wallet = PortfolioWalletRuntime(
                req.account_id,
                {(Exchange.BINANCE, market)},
                {(Exchange.BINANCE, market, 1001): wallet},
            )
            user_strategy = engine.create_strategy(
                user_id=req.user_id,
                strategy_path=req.strategy_path,
                wallet=portfolio_wallet,
                order_client=order_client,
                account_id=req.account_id,
            )
            ts = req.timescale.model_dump() if req.timescale else {}
            config = TimescaleConfig.from_dict(ts) if ts else TimescaleConfig()
            loop = BacktestDataLoop(service=engine, config=config)
            n = _run_backtest_simple(
                loop,
                symbol=req.symbol,
                start_time_ms=req.start_time_ms,
                end_time_ms=req.end_time_ms,
                interval=req.interval,
                market=market,
            )
            return BacktestRunResponse(ok=True, bars_processed=n, message="completed")
        except Exception as ex:
            raise HTTPException(status_code=400, detail=str(ex)) from ex

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def get_app() -> Any:
    """ASGI app for uvicorn: ``uvicorn strategy_service.http_app:get_app``."""
    return create_app()
