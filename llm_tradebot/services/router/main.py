"""Deterministic router service that tails feature snapshots and emits target exposure decisions."""

import json
import logging
import signal
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from llm_tradebot.core.config import get_settings
from llm_tradebot.core.logging import configure_logging

_TRIGGER_INTERVAL = "5m"
_POLL_SLEEP_S = 0.5
_WAIT_LOG_POLL_INTERVAL = 20


@dataclass(slots=True)
class RouterState:
    """In-memory anti-churn state for a symbol."""

    current_target: int = 0
    hold_bars: int = 0


class DecisionWriter:
    """Simple JSONL writer for deterministic router decisions."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._file: TextIO | None = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8")

    def write(self, decision: dict[str, Any]) -> None:
        if self._file is None:
            raise RuntimeError("decision writer is not open")
        line = json.dumps(decision, ensure_ascii=True, separators=(",", ":"))
        self._file.write(line + "\n")
        self._file.flush()

    def close(self) -> None:
        if self._file is None:
            return
        self._file.close()
        self._file = None


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_ready(snapshot: dict[str, Any]) -> dict[str, bool]:
    ready = {"5m": False, "15m": False, "1h": False}
    raw_ready = snapshot.get("ready")
    if not isinstance(raw_ready, dict):
        return ready

    for key in ready:
        ready[key] = raw_ready.get(key) is True
    return ready


def _candidate_target(
    bias: int,
    dev_from_ema20_5m: float | None,
    dev_threshold: float,
) -> tuple[int, str]:
    if bias == 0:
        return 0, "insufficient_history"

    if dev_from_ema20_5m is None:
        return 0, "insufficient_history"

    if bias == 1:
        if dev_from_ema20_5m <= -dev_threshold:
            return 1, "trend_pullback_entry"
        return 0, "trend_bias_no_pullback"

    if dev_from_ema20_5m >= dev_threshold:
        return -1, "trend_pullback_entry"
    return 0, "trend_bias_no_pullback"


def _decide_base(snapshot: dict[str, Any], dev_threshold: float) -> tuple[int, int, str, dict[str, Any]]:
    ready = _extract_ready(snapshot)
    trend_dir_raw = snapshot.get("trend_dir_1h")
    trend_dir_1h = trend_dir_raw.lower() if isinstance(trend_dir_raw, str) else None
    dev_from_ema20_5m = _as_float(snapshot.get("dev_from_ema20_5m"))

    if ready["1h"] and trend_dir_1h == "up":
        bias = 1
    elif ready["1h"] and trend_dir_1h == "down":
        bias = -1
    else:
        bias = 0

    target, reason = _candidate_target(
        bias=bias,
        dev_from_ema20_5m=dev_from_ema20_5m,
        dev_threshold=dev_threshold,
    )
    inputs_used = {
        "trend_dir_1h": trend_dir_1h,
        "dev_from_ema20_5m": dev_from_ema20_5m,
        "ready": ready,
    }
    return bias, target, reason, inputs_used


def _apply_anti_churn(state: RouterState, target: int, min_hold_bars: int) -> bool:
    if target != state.current_target and (
        state.current_target == 0 or state.hold_bars >= min_hold_bars
    ):
        state.current_target = target
        state.hold_bars = 0
        return False

    state.hold_bars += 1
    return target != state.current_target


def _build_decision(
    snapshot: dict[str, Any],
    symbol: str,
    state: RouterState,
    min_hold_bars: int,
    dev_threshold: float,
) -> dict[str, Any]:
    bias, candidate_target, reason, inputs_used = _decide_base(
        snapshot=snapshot,
        dev_threshold=dev_threshold,
    )
    hold_locked = _apply_anti_churn(state=state, target=candidate_target, min_hold_bars=min_hold_bars)
    if hold_locked:
        reason = "hold_lock"

    return {
        "type": "router_decision",
        "symbol": symbol,
        "close_time_ms": _as_int(snapshot.get("close_time_ms")),
        "event_time_ms": _as_int(snapshot.get("event_time_ms")),
        "bias": bias,
        "target_exposure": state.current_target,
        "current_target": state.current_target,
        "hold_bars": state.hold_bars,
        "inputs_used": inputs_used,
        "reason": reason,
    }


def _read_new_lines(path: Path, offset: int, logger: logging.Logger) -> tuple[int, list[str]]:
    size = path.stat().st_size
    if size < offset:
        logger.info(
            "router_snapshot_file_truncated",
            extra={"path": str(path), "previous_offset": offset, "size": size},
        )
        offset = 0

    with path.open("r", encoding="utf-8") as file_obj:
        file_obj.seek(offset)
        lines = file_obj.readlines()
        return file_obj.tell(), lines


def _request_shutdown(shutdown_event: threading.Event, logger: logging.Logger, signal_name: str) -> None:
    if shutdown_event.is_set():
        return
    logger.info("router_shutdown_signal", extra={"signal": signal_name})
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


def main() -> int:
    """Run the deterministic router until interrupted."""

    settings = get_settings()
    configure_logging(settings.LOG_LEVEL)
    logger = logging.getLogger(__name__)
    shutdown_event = threading.Event()

    symbols = settings.router_symbols()
    if not symbols:
        logger.error("router_invalid_symbols")
        return 1

    min_hold_bars = max(0, settings.ROUTER_MIN_HOLD_BARS)
    dev_threshold = max(0.0, abs(settings.ROUTER_DEV_ENTRY_THRESHOLD))
    log_every_n = max(1, settings.ROUTER_LOG_EVERY_N)
    snapshot_path = Path(settings.ROUTER_SNAPSHOT_PATH)

    writer = DecisionWriter(settings.ROUTER_DECISION_PATH)
    try:
        writer.open()
    except OSError as exc:
        logger.error(
            "router_decision_path_error",
            extra={"path": settings.ROUTER_DECISION_PATH, "error": str(exc)},
        )
        return 1

    _install_signal_handlers(shutdown_event, logger)
    logger.info(
        "router_startup",
        extra={
            "symbols": list(symbols),
            "snapshot_path": settings.ROUTER_SNAPSHOT_PATH,
            "decision_path": settings.ROUTER_DECISION_PATH,
            "min_hold_bars": min_hold_bars,
            "dev_entry_threshold": dev_threshold,
            "log_every_n": log_every_n,
        },
    )

    allowed_symbols = set(symbols)
    symbol_state: dict[str, RouterState] = {}
    file_offset = 0
    wait_polls = 0
    decision_count = 0

    try:
        while not shutdown_event.is_set():
            if not snapshot_path.exists():
                wait_polls += 1
                if wait_polls % (_WAIT_LOG_POLL_INTERVAL * log_every_n) == 0:
                    logger.info(
                        "router_waiting_for_snapshot_file",
                        extra={"path": str(snapshot_path)},
                    )
                shutdown_event.wait(_POLL_SLEEP_S)
                continue

            wait_polls = 0
            try:
                file_offset, lines = _read_new_lines(path=snapshot_path, offset=file_offset, logger=logger)
            except OSError as exc:
                logger.warning(
                    "router_snapshot_read_failed",
                    extra={"path": str(snapshot_path), "error": str(exc)},
                )
                shutdown_event.wait(_POLL_SLEEP_S)
                continue

            if not lines:
                shutdown_event.wait(_POLL_SLEEP_S)
                continue

            for raw_line in lines:
                if shutdown_event.is_set():
                    break

                line = raw_line.strip()
                if not line:
                    continue

                try:
                    snapshot = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("router_snapshot_invalid_json")
                    continue
                if not isinstance(snapshot, dict):
                    continue
                if snapshot.get("type") != "feature_snapshot":
                    continue

                interval_raw = snapshot.get("interval")
                if isinstance(interval_raw, str) and interval_raw.lower() != _TRIGGER_INTERVAL:
                    continue

                symbol_raw = snapshot.get("symbol")
                symbol = str(symbol_raw).upper() if symbol_raw is not None else ""
                if not symbol:
                    logger.warning("router_snapshot_missing_symbol")
                    continue
                if symbol not in allowed_symbols:
                    continue

                state = symbol_state.setdefault(symbol, RouterState())
                decision = _build_decision(
                    snapshot=snapshot,
                    symbol=symbol,
                    state=state,
                    min_hold_bars=min_hold_bars,
                    dev_threshold=dev_threshold,
                )

                try:
                    writer.write(decision)
                except OSError as exc:
                    logger.error(
                        "router_decision_write_failed",
                        extra={"path": str(writer.path), "error": str(exc), "symbol": symbol},
                    )
                    continue

                decision_count += 1
                logger.info("router_decision", extra=decision)
    finally:
        writer.close()

    logger.info("router_shutdown", extra={"decisions_written": decision_count})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
