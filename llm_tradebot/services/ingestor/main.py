"""Binance Futures websocket ingestor that emits 5m close events and caches recent bars."""

import asyncio
import inspect
import json
import logging
import signal
import sys
from collections import deque
from dataclasses import asdict
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from llm_tradebot.core.config import Settings, get_settings
from llm_tradebot.core.logging import configure_logging
from llm_tradebot.core.types import BarCloseEvent

_EXCHANGE = "binance_futures"
_EMIT_INTERVAL = "5m"
_RECONNECT_INITIAL_BACKOFF_S = 1.0
_RECONNECT_MAX_BACKOFF_S = 30.0
_WS_PING_INTERVAL_S = 30
_WS_RECV_TIMEOUT_S = 1.0

_BAR_BUFFERS: dict[tuple[str, str], deque[BarCloseEvent]] = {}


def get_bar_buffer(symbol: str, interval: str) -> tuple[BarCloseEvent, ...]:
    """Return a snapshot of cached closed bars for the given symbol and interval."""

    key = (symbol.upper(), interval.lower())
    if key not in _BAR_BUFFERS:
        return ()
    return tuple(_BAR_BUFFERS[key])


def _subscribe_streams(symbols: tuple[str, ...], intervals: tuple[str, ...]) -> list[str]:
    return [f"{symbol.lower()}@kline_{interval}" for symbol in symbols for interval in intervals]


def _append_closed_bar(event: BarCloseEvent, window: int) -> None:
    key = (event.symbol, event.interval)
    bars = _BAR_BUFFERS.get(key)
    if bars is None:
        bars = deque(maxlen=window)
        _BAR_BUFFERS[key] = bars
    elif bars.maxlen != window:
        bars = deque(bars, maxlen=window)
        _BAR_BUFFERS[key] = bars
    bars.append(event)


def _websocket_connect_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {"ping_interval": _WS_PING_INTERVAL_S}
    if "proxy" in inspect.signature(websockets.connect).parameters:
        kwargs["proxy"] = None
    return kwargs


def _build_bar_close_event(payload: dict[str, Any]) -> BarCloseEvent | None:
    if "data" in payload and isinstance(payload["data"], dict):
        payload = payload["data"]

    if payload.get("e") != "kline":
        return None

    kline = payload.get("k")
    if not isinstance(kline, dict) or not kline.get("x"):
        return None

    try:
        symbol = str(payload.get("s") or kline["s"]).upper()
        interval = str(kline["i"]).lower()
        open_time_ms = int(kline["t"])
        close_time_ms = int(kline["T"])
        event_time_ms = int(payload["E"])
    except (KeyError, TypeError, ValueError):
        return None

    return BarCloseEvent(
        type="bar_close",
        exchange=_EXCHANGE,
        symbol=symbol,
        interval=interval,
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        o=str(kline.get("o", "")),
        h=str(kline.get("h", "")),
        l=str(kline.get("l", "")),
        c=str(kline.get("c", "")),
        v=str(kline.get("v", "")),
        event_time_ms=event_time_ms,
    )


def _emit_event(event: BarCloseEvent) -> None:
    line = json.dumps(asdict(event), ensure_ascii=True, separators=(",", ":"))
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _request_shutdown(
    shutdown_event: asyncio.Event, logger: logging.Logger, signal_name: str
) -> None:
    if shutdown_event.is_set():
        return
    logger.info("ingestor_shutdown_signal", extra={"signal": signal_name})
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
    window: int,
) -> None:
    symbols = settings.ingest_symbols()
    intervals = settings.ingest_intervals()
    streams = _subscribe_streams(symbols, intervals)

    async with websockets.connect(
        settings.BINANCE_FUTURES_WS_URL,
        **_websocket_connect_kwargs(),
    ) as ws:
        logger.info(
            "ingestor_connected",
            extra={"url": settings.BINANCE_FUTURES_WS_URL, "stream_count": len(streams)},
        )

        await ws.send(
            json.dumps(
                {"method": "SUBSCRIBE", "params": streams, "id": 1},
                ensure_ascii=True,
                separators=(",", ":"),
            )
        )
        logger.info("ingestor_subscribed", extra={"streams": streams})

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
                logger.warning("ingestor_invalid_json_message")
                continue

            if not isinstance(payload, dict):
                continue

            if payload.get("result") is None and payload.get("id") == 1:
                logger.info("ingestor_subscribe_ack")
                continue

            event = _build_bar_close_event(payload)
            if event is None:
                continue

            _append_closed_bar(event, window=window)

            if event.interval == _EMIT_INTERVAL:
                _emit_event(event)


async def _run() -> int:
    settings = get_settings()
    configure_logging(settings.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    shutdown_event = asyncio.Event()

    symbols = settings.ingest_symbols()
    intervals = settings.ingest_intervals()
    if not symbols:
        logger.error("ingestor_invalid_symbols")
        return 1
    if not intervals:
        logger.error("ingestor_invalid_intervals")
        return 1

    _install_signal_handlers(shutdown_event, logger)
    window = max(1, settings.INGEST_WINDOW)
    logger.info(
        "ingestor_startup",
        extra={
            "exchange": _EXCHANGE,
            "symbols": list(symbols),
            "intervals": list(intervals),
            "window": window,
            "url": settings.BINANCE_FUTURES_WS_URL,
        },
    )

    backoff_s = _RECONNECT_INITIAL_BACKOFF_S
    while not shutdown_event.is_set():
        try:
            await _consume_stream(
                settings=settings,
                logger=logger,
                shutdown_event=shutdown_event,
                window=window,
            )
            backoff_s = _RECONNECT_INITIAL_BACKOFF_S
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if shutdown_event.is_set():
                break
            logger.warning(
                "ingestor_connection_lost",
                extra={"error": str(exc), "reconnect_in_s": backoff_s},
            )
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=backoff_s)
            except asyncio.TimeoutError:
                pass
            backoff_s = min(backoff_s * 2, _RECONNECT_MAX_BACKOFF_S)

    logger.info("ingestor_shutdown")
    return 0


def main() -> int:
    """Run the ingestor process until interrupted."""

    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
