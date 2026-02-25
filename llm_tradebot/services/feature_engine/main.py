"""Binance Futures feature engine that computes deterministic multi-timeframe snapshots."""

import asyncio
import inspect
import json
import logging
import math
import signal
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, TextIO

import websockets
from websockets.exceptions import ConnectionClosed

from llm_tradebot.core.config import Settings, get_settings
from llm_tradebot.core.logging import configure_logging

_EXCHANGE = "binance_futures"
_TRIGGER_INTERVAL = "5m"
_REQUIRED_INTERVALS = {"5m", "15m", "1h"}
_RECONNECT_INITIAL_BACKOFF_S = 1.0
_RECONNECT_MAX_BACKOFF_S = 30.0
_WS_PING_INTERVAL_S = 30
_WS_RECV_TIMEOUT_S = 1.0

_BAR_BUFFERS: dict[tuple[str, str], deque["Candle"]] = {}


@dataclass(frozen=True, slots=True)
class Candle:
    """Normalized closed-candle payload."""

    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    o: float
    h: float
    l: float
    c: float
    v: float
    event_time_ms: int


class SnapshotWriter:
    """Simple JSONL writer for deterministic feature snapshot persistence."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._file: TextIO | None = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")

    def write(self, snapshot: dict[str, Any]) -> None:
        if self._file is None:
            raise RuntimeError("snapshot writer is not open")
        line = json.dumps(snapshot, ensure_ascii=True, separators=(",", ":"))
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        if self._file is None:
            return
        self._file.close()
        self._file = None


def _subscribe_streams(symbols: tuple[str, ...], intervals: tuple[str, ...]) -> list[str]:
    return [f"{symbol.lower()}@kline_{interval}" for symbol in symbols for interval in intervals]


def _append_closed_bar(candle: Candle, window: int) -> None:
    key = (candle.symbol, candle.interval)
    bars = _BAR_BUFFERS.get(key)
    if bars is None:
        bars = deque(maxlen=window)
        _BAR_BUFFERS[key] = bars
    elif bars.maxlen != window:
        bars = deque(bars, maxlen=window)
        _BAR_BUFFERS[key] = bars
    bars.append(candle)


def _websocket_connect_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {"ping_interval": _WS_PING_INTERVAL_S}
    if "proxy" in inspect.signature(websockets.connect).parameters:
        kwargs["proxy"] = None
    return kwargs


def _build_closed_candle(payload: dict[str, Any]) -> Candle | None:
    if "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]

    if payload.get("e") != "kline":
        return None

    kline = payload.get("k")
    if not isinstance(kline, dict) or not kline.get("x"):
        return None

    try:
        return Candle(
            symbol=str(payload.get("s") or kline["s"]).upper(),
            interval=str(kline["i"]).lower(),
            open_time_ms=int(kline["t"]),
            close_time_ms=int(kline["T"]),
            o=float(kline["o"]),
            h=float(kline["h"]),
            l=float(kline["l"]),
            c=float(kline["c"]),
            v=float(kline["v"]),
            event_time_ms=int(payload["E"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _ema(values: Sequence[float], period: int) -> float | None:
    if period <= 0 or len(values) < period:
        return None

    alpha = 2.0 / (period + 1.0)
    ema_value = sum(values[:period]) / float(period)
    for value in values[period:]:
        ema_value = (value * alpha) + (ema_value * (1.0 - alpha))
    return ema_value


def _atr(bars: Sequence[Candle], period: int) -> float | None:
    if period <= 0 or len(bars) < period + 1:
        return None

    true_ranges: list[float] = []
    for idx in range(1, len(bars)):
        current = bars[idx]
        prev_close = bars[idx - 1].c
        tr = max(
            current.h - current.l,
            abs(current.h - prev_close),
            abs(current.l - prev_close),
        )
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    atr_value = sum(true_ranges[:period]) / float(period)
    for tr in true_ranges[period:]:
        atr_value = ((atr_value * (period - 1)) + tr) / float(period)

    return atr_value


def _log_return(current: float, previous: float) -> float | None:
    if current <= 0.0 or previous <= 0.0:
        return None
    return math.log(current / previous)


def _simple_return(current: float, previous: float) -> float | None:
    if previous == 0.0:
        return None
    return (current / previous) - 1.0


def _compute_feature_snapshot(symbol: str, regime_threshold: float) -> dict[str, Any] | None:
    bars_5m = _BAR_BUFFERS.get((symbol, "5m"))
    bars_15m = _BAR_BUFFERS.get((symbol, "15m"))
    bars_1h = _BAR_BUFFERS.get((symbol, "1h"))
    if bars_5m is None or bars_15m is None or bars_1h is None:
        return None

    bars_5m_seq = tuple(bars_5m)
    bars_15m_seq = tuple(bars_15m)
    bars_1h_seq = tuple(bars_1h)

    closes_5m = [bar.c for bar in bars_5m_seq]
    closes_15m = [bar.c for bar in bars_15m_seq]
    closes_1h = [bar.c for bar in bars_1h_seq]

    latest_close_5m = closes_5m[-1] if closes_5m else None
    latest_close_15m = closes_15m[-1] if closes_15m else None
    latest_close_1h = closes_1h[-1] if closes_1h else None
    if latest_close_5m is None or latest_close_15m is None or latest_close_1h is None:
        return None

    ema_20_1h = _ema(closes_1h, period=20)
    ema_50_1h = _ema(closes_1h, period=50)
    atr_1h = _atr(bars_1h_seq, period=14)

    ema_20_15m = _ema(closes_15m, period=20)
    ema_50_15m = _ema(closes_15m, period=50)
    momentum_15m = (
        _simple_return(closes_15m[-1], closes_15m[-4]) if len(closes_15m) >= 4 else None
    )

    ema_20_5m = _ema(closes_5m, period=20)
    atr_5m = _atr(bars_5m_seq, period=14)
    ret_5m = _log_return(closes_5m[-1], closes_5m[-2]) if len(closes_5m) >= 2 else None
    ret_5m_3 = _log_return(closes_5m[-1], closes_5m[-4]) if len(closes_5m) >= 4 else None

    required = (
        ema_20_1h,
        ema_50_1h,
        atr_1h,
        ema_20_15m,
        ema_50_15m,
        momentum_15m,
        ema_20_5m,
        atr_5m,
        ret_5m,
        ret_5m_3,
    )
    if any(value is None for value in required):
        return None
    if latest_close_1h <= 0.0 or latest_close_15m <= 0.0 or latest_close_5m <= 0.0:
        return None
    if ema_20_5m == 0.0:
        return None

    trend_strength_1h = abs(ema_20_1h - ema_50_1h) / latest_close_1h
    atr_pct_1h = atr_1h / latest_close_1h
    regime_strength_15m = abs(ema_20_15m - ema_50_15m) / latest_close_15m
    atr_pct_5m = atr_5m / latest_close_5m

    trigger_bar = bars_5m_seq[-1]
    return {
        "type": "feature_snapshot",
        "exchange": _EXCHANGE,
        "symbol": symbol,
        "interval": _TRIGGER_INTERVAL,
        "close_time_ms": trigger_bar.close_time_ms,
        "event_time_ms": trigger_bar.event_time_ms,
        "ema_20_1h": ema_20_1h,
        "ema_50_1h": ema_50_1h,
        "trend_dir_1h": "up" if ema_20_1h > ema_50_1h else "down",
        "trend_strength_1h": trend_strength_1h,
        "atr_pct_1h": atr_pct_1h,
        "ema_20_15m": ema_20_15m,
        "ema_50_15m": ema_50_15m,
        "regime_15m": "trend" if regime_strength_15m > regime_threshold else "range",
        "momentum_15m": momentum_15m,
        "ret_5m": ret_5m,
        "ret_5m_3": ret_5m_3,
        "dev_from_ema20_5m": (latest_close_5m - ema_20_5m) / ema_20_5m,
        "atr_pct_5m": atr_pct_5m,
    }


def _request_shutdown(
    shutdown_event: asyncio.Event, logger: logging.Logger, signal_name: str
) -> None:
    if shutdown_event.is_set():
        return
    logger.info("feature_engine_shutdown_signal", extra={"signal": signal_name})
    shutdown_event.set()


def _install_signal_handlers(shutdown_event: asyncio.Event, logger: logging.Logger) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                _request_shutdown,
                shutdown_event,
                logger,
                sig.name,
            )
        except NotImplementedError:
            signal_name = sig.name
            signal.signal(
                sig,
                lambda *_, signal_name=signal_name: _request_shutdown(
                    shutdown_event, logger, signal_name
                ),
            )


async def _consume_stream(
    settings: Settings,
    logger: logging.Logger,
    shutdown_event: asyncio.Event,
    writer: SnapshotWriter,
    window: int,
    regime_threshold: float,
) -> None:
    symbols = settings.feature_symbols()
    intervals = settings.feature_intervals()
    streams = _subscribe_streams(symbols, intervals)

    async with websockets.connect(
        settings.BINANCE_FUTURES_WS_URL,
        **_websocket_connect_kwargs(),
    ) as ws:
        logger.info(
            "feature_engine_connected",
            extra={"url": settings.BINANCE_FUTURES_WS_URL, "stream_count": len(streams)},
        )
        await ws.send(
            json.dumps(
                {"method": "SUBSCRIBE", "params": streams, "id": 1},
                ensure_ascii=True,
                separators=(",", ":"),
            )
        )
        logger.info("feature_engine_subscribed", extra={"streams": streams})

        while not shutdown_event.is_set():
            try:
                raw_message = await asyncio.wait_for(ws.recv(), timeout=_WS_RECV_TIMEOUT_S)
            except asyncio.TimeoutError:
                continue
            except ConnectionClosed:
                raise

            try:
                payload = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning("feature_engine_invalid_json_message")
                continue
            if not isinstance(payload, dict):
                continue

            if payload.get("result") is None and payload.get("id") == 1:
                logger.info("feature_engine_subscribe_ack")
                continue

            candle = _build_closed_candle(payload)
            if candle is None:
                continue

            _append_closed_bar(candle, window=window)

            if candle.interval != _TRIGGER_INTERVAL:
                continue

            snapshot = _compute_feature_snapshot(candle.symbol, regime_threshold=regime_threshold)
            if snapshot is None:
                logger.debug(
                    "feature_snapshot_skipped",
                    extra={"symbol": candle.symbol, "reason": "insufficient_data"},
                )
                continue

            try:
                writer.write(snapshot)
            except OSError as exc:
                logger.error(
                    "feature_snapshot_write_failed",
                    extra={"error": str(exc), "path": str(writer.path)},
                )
                continue

            logger.info(
                "feature_snapshot",
                extra={
                    "symbol": candle.symbol,
                    "close_time_ms": candle.close_time_ms,
                    "snapshot": snapshot,
                },
            )


async def _run() -> int:
    settings = get_settings()
    configure_logging(settings.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    shutdown_event = asyncio.Event()

    symbols = settings.feature_symbols()
    intervals = settings.feature_intervals()
    if not symbols:
        logger.error("feature_engine_invalid_symbols")
        return 1
    if not intervals:
        logger.error("feature_engine_invalid_intervals")
        return 1
    missing_intervals = _REQUIRED_INTERVALS.difference(intervals)
    if missing_intervals:
        logger.error(
            "feature_engine_missing_required_intervals",
            extra={"required": sorted(_REQUIRED_INTERVALS), "configured": list(intervals)},
        )
        return 1

    writer = SnapshotWriter(settings.FEATURE_SNAPSHOT_PATH)
    try:
        writer.open()
    except OSError as exc:
        logger.error(
            "feature_engine_snapshot_path_error",
            extra={"path": settings.FEATURE_SNAPSHOT_PATH, "error": str(exc)},
        )
        return 1

    _install_signal_handlers(shutdown_event, logger)
    window = max(1, settings.FEATURE_WINDOW)
    regime_threshold = max(0.0, settings.REGIME_TREND_THRESHOLD)
    logger.info(
        "feature_engine_startup",
        extra={
            "exchange": _EXCHANGE,
            "symbols": list(symbols),
            "intervals": list(intervals),
            "window": window,
            "regime_trend_threshold": regime_threshold,
            "snapshot_path": settings.FEATURE_SNAPSHOT_PATH,
            "url": settings.BINANCE_FUTURES_WS_URL,
        },
    )

    backoff_s = _RECONNECT_INITIAL_BACKOFF_S
    try:
        while not shutdown_event.is_set():
            try:
                await _consume_stream(
                    settings=settings,
                    logger=logger,
                    shutdown_event=shutdown_event,
                    writer=writer,
                    window=window,
                    regime_threshold=regime_threshold,
                )
                backoff_s = _RECONNECT_INITIAL_BACKOFF_S
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if shutdown_event.is_set():
                    break
                logger.warning(
                    "feature_engine_connection_lost",
                    extra={"error": str(exc), "reconnect_in_s": backoff_s},
                )
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=backoff_s)
                except asyncio.TimeoutError:
                    pass
                backoff_s = min(backoff_s * 2, _RECONNECT_MAX_BACKOFF_S)
    finally:
        writer.close()

    logger.info("feature_engine_shutdown")
    return 0


def main() -> int:
    """Run the feature engine process until interrupted."""

    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
