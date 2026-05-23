#!/usr/bin/env python3
"""
验证 market_data → algo 数据流是否通畅。

行为依照 `openspec/changes/verify-algo-flow/{proposal.md,tasks.md}`：
1. 从 TimescaleDB 拉取最近 100 根 BTCUSDT 1m K 线
2. 转为 pandas.DataFrame（仅要求 open/high/low/close/volume 五列）
3. 计算 rsi/macd/bollinger_bands/atr 并断言输出格式与数值范围
4. 输出运行日志到 `openspec/changes/verify-algo-flow/`
"""

from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd


WS_ROOT = Path(__file__).resolve().parent.parent  # /home/.../WorkSpace
STRATEGY_LIBRARY_ROOT = WS_ROOT / "strategy-library"
sys.path.insert(0, str(STRATEGY_LIBRARY_ROOT))

from market_data.backtest import BacktestDataSource
from market_data.config import TimescaleConfig
from algo import atr, bollinger_bands, macd, rsi


@dataclass(frozen=True)
class VerifyConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "binance"
    user: str = "postgres"
    password: str = "postgres"

    symbol: str = "BTCUSDT"
    interval: str = "1m"
    market: str = "futures"
    row_count: int = 100

    # algo 默认参数（spec 强约定）
    rsi_length: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bb_length: int = 20
    bb_std: float = 2.0
    atr_length: int = 14


def _setup_logger(output_file: Path) -> logging.Logger:
    logger = logging.getLogger("verify_algo_flow")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(output_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def _to_ohlcv_df(klines: List[object]) -> pd.DataFrame:
    records = [
        {
            "open": float(k.open),
            "high": float(k.high),
            "low": float(k.low),
            "close": float(k.close),
            "volume": float(k.volume),
        }
        for k in klines
    ]
    df = pd.DataFrame(records, columns=["open", "high", "low", "close", "volume"])
    return df.astype(float)


def _is_finite_series(s: pd.Series) -> bool:
    return s[np.isfinite(s.to_numpy())].shape[0] > 0


def _assert_ohlc_sane(df: pd.DataFrame) -> None:
    # 可选质量检查：low <= min(open,close) 且 high >= max(open,close)
    min_oc = df[["open", "close"]].min(axis=1)
    max_oc = df[["open", "close"]].max(axis=1)
    ok = (df["low"] <= min_oc + 1e-12).all() and (df["high"] + 1e-12 >= max_oc).all()
    if not ok:
        raise AssertionError("OHLC 关系不一致：检查 low/high 相对 open/close。")


def main() -> None:
    config = VerifyConfig()
    repo_root = Path(__file__).resolve().parent
    output_path = repo_root / "openspec/changes/verify-algo-flow/verify_algo_flow_output.txt"
    logger = _setup_logger(output_path)

    logger.info("=== Algo Flow Verification ===")
    logger.info(f"Log file: {output_path}")

    ds = None
    try:
        logger.info("\n1. 初始化 BacktestDataSource...")
        ts_config = TimescaleConfig(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
        )
        ds = BacktestDataSource(ts_config)
        logger.info("   OK: BacktestDataSource 已创建")

        logger.info("\n2. 从 TimescaleDB 拉取最近 100 根 BTCUSDT 1m K 线...")
        now = datetime.now(tz=timezone.utc)
        start_time = int(datetime(now.year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end_time = int(now.timestamp() * 1000)

        t0 = time.time()
        rows = list(
            ds.get_klines(
                symbol=config.symbol,
                interval=config.interval,
                start_time=start_time,
                end_time=end_time,
                market=config.market,
            )
        )
        elapsed = time.time() - t0
        logger.info(f"   原始查询结果: {len(rows)} 条，耗时 {elapsed:.2f}s")

        if len(rows) == 0:
            raise RuntimeError("查询结果为 0：可能表不存在、库名/连接配置不对，或数据尚未写入。")

        # 规范要求：按 open_time 升序，取最近 100
        rows.sort(key=lambda k: k.open_time)
        if len(rows) < config.row_count:
            raise RuntimeError(f"数据不足：需要至少 {config.row_count} 根，实际只有 {len(rows)} 根。")
        rows = rows[-config.row_count :]
        logger.info(f"   使用最近 {config.row_count} 根（open_time 升序后截断）")

        first_open_time = rows[0].open_time
        last_open_time = rows[-1].open_time
        logger.info(f"   open_time: {first_open_time} .. {last_open_time}")

        logger.info("\n3. 转为 DataFrame (open/high/low/close/volume)...")
        df = _to_ohlcv_df(rows)
        logger.info(f"   OK: df.shape={df.shape}")
        logger.info(f"   df.columns={list(df.columns)}")
        logger.info(f"   df.tail(5):\n{df.tail(5)}")

        # 指标计算前的可选质量检查
        _assert_ohlc_sane(df)

        logger.info("\n4. 计算指标并打印尾部...")
        rsi_s = rsi(df, length=config.rsi_length)
        macd_df = macd(df, fast=config.macd_fast, slow=config.macd_slow, signal=config.macd_signal)
        bb_df = bollinger_bands(df, length=config.bb_length, std=config.bb_std)
        atr_s = atr(df, length=config.atr_length)

        logger.info(f"   RSI.tail(5):\n{rsi_s.tail(5)}")
        logger.info(f"   MACD.tail(5):\n{macd_df.tail(5)}")
        logger.info(f"   BB.tail(5):\n{bb_df.tail(5)}")
        logger.info(f"   ATR.tail(5):\n{atr_s.tail(5)}")

        logger.info("\n5. 断言校验输出格式与数值范围...")
        # 5.1 RSI
        if not isinstance(rsi_s, pd.Series):
            raise AssertionError("rsi(df) 应返回 pd.Series。")
        if len(df) >= config.rsi_length + 1:
            last_rsi = rsi_s.iloc[-1]
            if not np.isfinite(last_rsi):
                raise AssertionError("RSI 最后一行应为有限数。")

        finite_rsi = rsi_s[np.isfinite(rsi_s.to_numpy())].to_numpy()
        if finite_rsi.size > 0:
            eps = 1e-3  # 允许极小边界噪声
            if finite_rsi.min() < -eps or finite_rsi.max() > 100 + eps:
                raise AssertionError(f"RSI 范围异常：min={finite_rsi.min()}, max={finite_rsi.max()}")

        # 5.2 MACD
        expected_macd_cols = ["macd_macd", "macd_signal", "macd_hist"]
        if not isinstance(macd_df, pd.DataFrame):
            raise AssertionError("macd(df) 应返回 pd.DataFrame。")
        if list(macd_df.columns) != expected_macd_cols:
            raise AssertionError(f"macd 列名不一致：got={list(macd_df.columns)} expected={expected_macd_cols}")
        if len(macd_df) != len(df):
            raise AssertionError("macd 输出行数与输入 df 不一致。")
        if len(df) >= config.macd_slow + config.macd_signal:
            last_macd = macd_df.iloc[-1][expected_macd_cols].to_numpy(dtype=float)
            if not np.isfinite(last_macd).all():
                raise AssertionError("MACD 最后一行三列应为有限数。")

        # 5.3 Bollinger Bands
        expected_bb_cols = ["bb_lower", "bb_middle", "bb_upper"]
        if not isinstance(bb_df, pd.DataFrame):
            raise AssertionError("bollinger_bands(df) 应返回 pd.DataFrame。")
        if list(bb_df.columns) != expected_bb_cols:
            raise AssertionError(f"BB 列名不一致：got={list(bb_df.columns)} expected={expected_bb_cols}")
        if len(bb_df) != len(df):
            raise AssertionError("bollinger_bands 输出行数与输入 df 不一致。")
        mask = np.isfinite(bb_df[expected_bb_cols]).all(axis=1)
        if mask.any():
            checked = bb_df.loc[mask, expected_bb_cols]
            if not ((checked["bb_lower"] <= checked["bb_middle"]) & (checked["bb_middle"] <= checked["bb_upper"])).all():
                raise AssertionError("布林带不等式失败：需要 bb_lower <= bb_middle <= bb_upper。")

        # 5.4 ATR
        if not isinstance(atr_s, pd.Series):
            raise AssertionError("atr(df) 应返回 pd.Series。")
        if len(df) >= config.atr_length + 1:
            last_atr = atr_s.iloc[-1]
            if not np.isfinite(last_atr) or last_atr <= 0:
                raise AssertionError("ATR 最后一行应为有限正数。")
        finite_atr = atr_s[np.isfinite(atr_s.to_numpy())].to_numpy()
        if finite_atr.size > 0 and (finite_atr <= 0).any():
            raise AssertionError(f"ATR 存在非正值：min={finite_atr.min()}, max={finite_atr.max()}")

        last_macd_macd, last_macd_signal, last_macd_hist = macd_df.iloc[-1][expected_macd_cols].tolist()
        last_bb_lower, last_bb_middle, last_bb_upper = bb_df.iloc[-1][expected_bb_cols].tolist()
        logger.info("\n=== 汇总（最后一行）===")
        logger.info(
            f"RSI={float(rsi_s.iloc[-1]):.6f} | "
            f"MACD(macd={float(last_macd_macd):.6f}, signal={float(last_macd_signal):.6f}, hist={float(last_macd_hist):.6f}) | "
            f"BB(lower={float(last_bb_lower):.6f}, middle={float(last_bb_middle):.6f}, upper={float(last_bb_upper):.6f}) | "
            f"ATR={float(atr_s.iloc[-1]):.6f}"
        )

        logger.info("\n=== 验证通过 ===")
        sys.exit(0)
    except Exception as e:
        logger.exception("\n=== 验证失败 ===")
        logger.info(f"Error: {e}")
        sys.exit(1)
    finally:
        if ds is not None:
            try:
                ds.close()
            except Exception:
                # ignore close errors
                pass


if __name__ == "__main__":
    main()
