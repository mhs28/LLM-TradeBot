"""Paper trading engine that tails router decisions and feature snapshots."""

import json
import logging
import math
import signal
import threading
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from llm_tradebot.core.config import get_settings
from llm_tradebot.core.logging import configure_logging

_POLL_SLEEP_S = 0.5
_WAIT_LOG_POLL_INTERVAL = 20
_PRICE_CACHE_LIMIT_PER_SYMBOL = 2000


@dataclass(slots=True)
class TailState:
    """Mutable file tail offset and wait-tracking state."""

    path: Path
    stream_name: str
    offset: int = 0
    wait_polls: int = 0


@dataclass(slots=True)
class PendingDecision:
    """Minimal normalized router decision payload."""

    symbol: str
    close_time_ms: int
    event_time_ms: int | None
    target_exposure: int
    reason: str | None


@dataclass(slots=True)
class PositionState:
    """Per-symbol paper position state."""

    position: int = 0
    entry_price: float | None = None
    entry_time_ms: int | None = None
    realized_pnl: float = 0.0
    cum_fees: float = 0.0
    trade_count: int = 0


class PaperTradeWriter:
    """Simple JSONL writer for deterministic paper trade records."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._file: TextIO | None = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")

    def write(self, trade: dict[str, Any]) -> None:
        if self._file is None:
            raise RuntimeError("paper trade writer is not open")
        line = json.dumps(trade, ensure_ascii=True, separators=(",", ":"))
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        if self._file is None:
            return
        self._file.close()
        self._file = None


class PriceCache:
    """Bounded in-memory close-price cache keyed by symbol + close_time_ms."""

    def __init__(self, limit_per_symbol: int) -> None:
        self.limit_per_symbol = max(1, limit_per_symbol)
        self._by_symbol: dict[str, OrderedDict[int, float]] = {}

    def put(self, symbol: str, close_time_ms: int, close_price: float) -> None:
        per_symbol = self._by_symbol.setdefault(symbol, OrderedDict())
        if close_time_ms in per_symbol:
            del per_symbol[close_time_ms]
        per_symbol[close_time_ms] = close_price
        while len(per_symbol) > self.limit_per_symbol:
            per_symbol.popitem(last=False)

    def get(self, symbol: str, close_time_ms: int) -> float | None:
        per_symbol = self._by_symbol.get(symbol)
        if per_symbol is None:
            return None
        return per_symbol.get(close_time_ms)


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric_value):
        return None
    return numeric_value


def _normalize_target(value: Any) -> int | None:
    target = _as_int(value)
    if target is None:
        return None
    if target > 0:
        return 1
    if target < 0:
        return -1
    return 0


def _normalize_symbol(value: Any) -> str:
    return str(value).upper().strip() if value is not None else ""


def _read_new_lines(tail: TailState, logger: logging.Logger) -> list[str]:
    if not tail.path.exists():
        tail.wait_polls += 1
        if tail.wait_polls % _WAIT_LOG_POLL_INTERVAL == 0:
            logger.info(
                "paper_trader_waiting_for_file",
                extra={"stream": tail.stream_name, "path": str(tail.path)},
            )
        return []

    try:
        size = tail.path.stat().st_size
    except OSError as exc:
        tail.wait_polls += 1
        logger.warning(
            "paper_trader_stat_failed",
            extra={"stream": tail.stream_name, "path": str(tail.path), "error": str(exc)},
        )
        return []

    if size < tail.offset:
        logger.info(
            "paper_trader_file_truncated",
            extra={
                "stream": tail.stream_name,
                "path": str(tail.path),
                "previous_offset": tail.offset,
                "size": size,
            },
        )
        tail.offset = 0

    if size == 0:
        tail.wait_polls += 1
        if tail.wait_polls % _WAIT_LOG_POLL_INTERVAL == 0:
            logger.info(
                "paper_trader_waiting_for_data",
                extra={"stream": tail.stream_name, "path": str(tail.path)},
            )
        return []

    try:
        with tail.path.open("r", encoding="utf-8") as file_obj:
            file_obj.seek(tail.offset)
            lines = file_obj.readlines()
            tail.offset = file_obj.tell()
    except OSError as exc:
        tail.wait_polls += 1
        logger.warning(
            "paper_trader_read_failed",
            extra={"stream": tail.stream_name, "path": str(tail.path), "error": str(exc)},
        )
        return []

    if lines:
        tail.wait_polls = 0
    else:
        tail.wait_polls += 1
    return lines


def _parse_json_line(raw_line: str, logger: logging.Logger, stream_name: str) -> dict[str, Any] | None:
    line = raw_line.strip()
    if not line:
        return None

    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        logger.warning("paper_trader_invalid_json", extra={"stream": stream_name})
        return None

    if not isinstance(payload, dict):
        return None
    return payload


def _cache_snapshot_price(
    payload: dict[str, Any],
    allowed_symbols: set[str],
    price_cache: PriceCache,
) -> tuple[str, int, float] | None:
    if payload.get("type") != "feature_snapshot":
        return None

    interval_raw = payload.get("interval")
    interval = interval_raw.lower() if isinstance(interval_raw, str) else None
    if interval is not None and interval != "5m":
        return None

    symbol = _normalize_symbol(payload.get("symbol"))
    if not symbol or symbol not in allowed_symbols:
        return None

    close_time_ms = _as_int(payload.get("close_time_ms"))
    close_price = _as_float(payload.get("c"))
    if close_time_ms is None or close_price is None:
        return None
    if close_price <= 0.0:
        return None

    price_cache.put(symbol=symbol, close_time_ms=close_time_ms, close_price=close_price)
    return symbol, close_time_ms, close_price


def _parse_decision(payload: dict[str, Any], allowed_symbols: set[str]) -> PendingDecision | None:
    if payload.get("type") != "router_decision":
        return None

    symbol = _normalize_symbol(payload.get("symbol"))
    if not symbol or symbol not in allowed_symbols:
        return None

    close_time_ms = _as_int(payload.get("close_time_ms"))
    target_exposure = _normalize_target(payload.get("target_exposure"))
    if close_time_ms is None or target_exposure is None:
        return None

    reason_raw = payload.get("reason")
    reason = str(reason_raw) if reason_raw is not None else None
    return PendingDecision(
        symbol=symbol,
        close_time_ms=close_time_ms,
        event_time_ms=_as_int(payload.get("event_time_ms")),
        target_exposure=target_exposure,
        reason=reason,
    )


def _request_shutdown(shutdown_event: threading.Event, logger: logging.Logger, signal_name: str) -> None:
    if shutdown_event.is_set():
        return
    logger.info("paper_trader_shutdown_signal", extra={"signal": signal_name})
    shutdown_event.set()


def _install_signal_handlers(shutdown_event: threading.Event, logger: logging.Logger) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal_name = sig.name
        signal.signal(
            sig,
            lambda *_args, signal_name=signal_name: _request_shutdown(
                shutdown_event,
                logger,
                signal_name,
            ),
        )


def _realized_pnl(from_position: int, entry_price: float, fill_price: float) -> float:
    if from_position == 1:
        return fill_price - entry_price
    if from_position == -1:
        return entry_price - fill_price
    return 0.0


def _execute_trade(
    decision: PendingDecision,
    fill_price: float,
    fee_rate: float,
    state: PositionState,
    logger: logging.Logger,
) -> dict[str, Any] | None:
    from_position = state.position
    to_position = decision.target_exposure
    if from_position == to_position:
        return None

    entry_price_for_record: float | None = None
    realized_pnl = 0.0
    if from_position == 0 and to_position == 1:
        action = "OPEN_LONG"
    elif from_position == 0 and to_position == -1:
        action = "OPEN_SHORT"
    elif to_position == 0:
        action = "CLOSE"
        entry_price = state.entry_price if state.entry_price is not None else fill_price
        if state.entry_price is None:
            logger.warning("paper_trader_missing_entry_price", extra={"symbol": decision.symbol})
        entry_price_for_record = entry_price
        realized_pnl = _realized_pnl(from_position=from_position, entry_price=entry_price, fill_price=fill_price)
    else:
        action = "FLIP"
        entry_price = state.entry_price if state.entry_price is not None else fill_price
        if state.entry_price is None:
            logger.warning("paper_trader_missing_entry_price", extra={"symbol": decision.symbol})
        entry_price_for_record = entry_price
        realized_pnl = _realized_pnl(from_position=from_position, entry_price=entry_price, fill_price=fill_price)

    fee = abs(to_position - from_position) * fill_price * fee_rate
    state.realized_pnl += realized_pnl
    state.cum_fees += fee
    state.trade_count += 1
    state.position = to_position
    if to_position == 0:
        state.entry_price = None
        state.entry_time_ms = None
    else:
        state.entry_price = fill_price
        state.entry_time_ms = decision.close_time_ms

    return {
        "type": "paper_trade",
        "symbol": decision.symbol,
        "action": action,
        "from_position": from_position,
        "to_position": to_position,
        "close_time_ms": decision.close_time_ms,
        "event_time_ms": decision.event_time_ms,
        "fill_price": fill_price,
        "entry_price": entry_price_for_record,
        "realized_pnl": realized_pnl,
        "cum_realized_pnl": state.realized_pnl,
        "fee": fee,
        "cum_fees": state.cum_fees,
        "reason": decision.reason,
    }


def _process_pending_decisions(
    pending_by_symbol: dict[str, deque[PendingDecision]],
    price_cache: PriceCache,
    position_by_symbol: dict[str, PositionState],
    fee_rate: float,
    writer: PaperTradeWriter,
    logger: logging.Logger,
) -> tuple[int, float, float, int]:
    trades_written = 0
    realized_delta = 0.0
    fees_delta = 0.0

    for symbol in list(pending_by_symbol.keys()):
        symbol_trades, symbol_realized, symbol_fees = _process_symbol_pending_decisions(
            symbol=symbol,
            pending_by_symbol=pending_by_symbol,
            price_cache=price_cache,
            position_by_symbol=position_by_symbol,
            fee_rate=fee_rate,
            writer=writer,
            logger=logger,
        )
        trades_written += symbol_trades
        realized_delta += symbol_realized
        fees_delta += symbol_fees

    pending_count = sum(len(queue) for queue in pending_by_symbol.values())
    return trades_written, realized_delta, fees_delta, pending_count


def _process_symbol_pending_decisions(
    symbol: str,
    pending_by_symbol: dict[str, deque[PendingDecision]],
    price_cache: PriceCache,
    position_by_symbol: dict[str, PositionState],
    fee_rate: float,
    writer: PaperTradeWriter,
    logger: logging.Logger,
) -> tuple[int, float, float]:
    queue = pending_by_symbol.get(symbol)
    if not queue:
        return 0, 0.0, 0.0

    trades_written = 0
    realized_delta = 0.0
    fees_delta = 0.0
    state = position_by_symbol.setdefault(symbol, PositionState())
    unresolved: deque[PendingDecision] = deque()
    resolvable: list[PendingDecision] = []

    while queue:
        decision = queue.popleft()
        fill_price = price_cache.get(symbol=decision.symbol, close_time_ms=decision.close_time_ms)
        if fill_price is None:
            unresolved.append(decision)
            continue
        resolvable.append(decision)

    resolvable.sort(key=lambda decision: decision.close_time_ms)
    for decision in resolvable:
        fill_price = price_cache.get(symbol=decision.symbol, close_time_ms=decision.close_time_ms)
        if fill_price is None:
            unresolved.append(decision)
            continue

        logger.info(
            "paper_trader_decision_resolved",
            extra={"symbol": decision.symbol, "close_time_ms": decision.close_time_ms},
        )
        trade = _execute_trade(
            decision=decision,
            fill_price=fill_price,
            fee_rate=fee_rate,
            state=state,
            logger=logger,
        )
        if trade is None:
            continue

        writer.write(trade)
        logger.info("paper_trade", extra=trade)
        trades_written += 1
        realized_delta += float(trade["realized_pnl"])
        fees_delta += float(trade["fee"])

    if unresolved:
        pending_by_symbol[symbol] = unresolved
    else:
        pending_by_symbol.pop(symbol, None)
    return trades_written, realized_delta, fees_delta


def main() -> int:
    """Run the paper trading service until interrupted."""

    settings = get_settings()
    configure_logging(settings.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    shutdown_event = threading.Event()

    symbols = settings.paper_symbols()
    if not symbols:
        logger.error("paper_trader_invalid_symbols")
        return 1
    allowed_symbols = set(symbols)

    decision_path = Path(settings.trader_decision_path())
    snapshot_path = Path(settings.trader_snapshot_path())
    fee_rate = max(0.0, abs(settings.PAPER_FEE_RATE))
    paper_start_balance = float(settings.PAPER_START_BALANCE)

    writer = PaperTradeWriter(settings.PAPER_TRADES_PATH)
    try:
        writer.open()
    except OSError as exc:
        logger.error(
            "paper_trader_output_path_error",
            extra={"path": settings.PAPER_TRADES_PATH, "error": str(exc)},
        )
        return 1

    _install_signal_handlers(shutdown_event, logger)
    logger.info(
        "paper_trader_startup",
        extra={
            "symbols": list(symbols),
            "decision_path": str(decision_path),
            "snapshot_path": str(snapshot_path),
            "paper_trades_path": settings.PAPER_TRADES_PATH,
            "start_balance": paper_start_balance,
            "fee_rate": fee_rate,
        },
    )

    decision_tail = TailState(path=decision_path, stream_name="router_decisions")
    snapshot_tail = TailState(path=snapshot_path, stream_name="feature_snapshots")
    pending_by_symbol: dict[str, deque[PendingDecision]] = defaultdict(deque)
    position_by_symbol: dict[str, PositionState] = {}
    price_cache = PriceCache(limit_per_symbol=_PRICE_CACHE_LIMIT_PER_SYMBOL)

    total_realized_pnl = 0.0
    total_fees = 0.0
    total_trades = 0

    try:
        while not shutdown_event.is_set():
            did_work = False

            snapshot_lines = _read_new_lines(tail=snapshot_tail, logger=logger)
            if snapshot_lines:
                did_work = True
            for raw_line in snapshot_lines:
                payload = _parse_json_line(
                    raw_line=raw_line,
                    logger=logger,
                    stream_name=snapshot_tail.stream_name,
                )
                if payload is None:
                    continue
                cached_price = _cache_snapshot_price(
                    payload=payload,
                    allowed_symbols=allowed_symbols,
                    price_cache=price_cache,
                )
                if cached_price is None:
                    continue
                symbol, close_time_ms, close_price = cached_price
                logger.info(
                    "paper_trader_price_cached",
                    extra={"symbol": symbol, "close_time_ms": close_time_ms, "c": close_price},
                )
                symbol_trades, symbol_realized, symbol_fees = _process_symbol_pending_decisions(
                    symbol=symbol,
                    pending_by_symbol=pending_by_symbol,
                    price_cache=price_cache,
                    position_by_symbol=position_by_symbol,
                    fee_rate=fee_rate,
                    writer=writer,
                    logger=logger,
                )
                if symbol_trades > 0:
                    total_trades += symbol_trades
                    total_realized_pnl += symbol_realized
                    total_fees += symbol_fees

            decision_lines = _read_new_lines(tail=decision_tail, logger=logger)
            if decision_lines:
                did_work = True
            for raw_line in decision_lines:
                payload = _parse_json_line(
                    raw_line=raw_line,
                    logger=logger,
                    stream_name=decision_tail.stream_name,
                )
                if payload is None:
                    continue
                decision = _parse_decision(payload=payload, allowed_symbols=allowed_symbols)
                if decision is None:
                    continue
                pending_by_symbol[decision.symbol].append(decision)

            trades_written, realized_delta, fees_delta, pending_count = _process_pending_decisions(
                pending_by_symbol=pending_by_symbol,
                price_cache=price_cache,
                position_by_symbol=position_by_symbol,
                fee_rate=fee_rate,
                writer=writer,
                logger=logger,
            )
            if trades_written > 0:
                did_work = True
                total_trades += trades_written
                total_realized_pnl += realized_delta
                total_fees += fees_delta

            if not did_work:
                shutdown_event.wait(_POLL_SLEEP_S)
                continue

            if pending_count > 0:
                logger.info("paper_trader_pending_buffer", extra={"pending_decisions": pending_count})
    finally:
        writer.close()

    logger.info(
        "paper_trader_shutdown",
        extra={
            "trades_written": total_trades,
            "realized_pnl": total_realized_pnl,
            "fees": total_fees,
            "paper_balance": paper_start_balance + total_realized_pnl - total_fees,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
