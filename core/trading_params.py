"""Dynamic trading parameters loaded from a JSON config.

The JSON is the source of truth for TP / SL / leverage / risk-per-trade.
The active market regime (``bull`` / ``bear``) is determined elsewhere
(BTC SMA50 vs SMA100 cross) and passed in to select the tier set.

The file is hot-reloaded on mtime change so edits are picked up without
restarting the long-running services.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from core.config import Settings, get_settings
from core.logger import logger

DEFAULT_REGIME = "bull"
VALID_REGIMES = ("bull", "bear")


def _resolve_params_path(config: Settings) -> Path:
    """Resolve the configured JSON file path relative to the bot package root."""
    raw = config.BOT_TRADING_PARAMS_FILE
    path = Path(raw)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parent.parent / path


class TradingParamsLoader:
    """Thread-safe loader with mtime-based hot reload."""

    def __init__(self, config: Settings | None = None) -> None:
        self._config = config or get_settings()
        self._path = _resolve_params_path(self._config)
        self._lock = threading.Lock()
        self._mtime: float | None = None
        self._data: dict[str, Any] | None = None

    @property
    def path(self) -> Path:
        return self._path

    def get(self) -> dict[str, Any]:
        """Return the parsed config, reloading when the file's mtime changes.

        If the file is missing or malformed, returns the last good copy, or
        an empty dict on the first failure. Callers must tolerate an empty
        dict and fall back to ``core.config.Settings`` defaults.
        """
        try:
            mtime = os.path.getmtime(self._path)
        except OSError as e:
            if self._data is None:
                logger.error(
                    f"Trading params file not accessible at {self._path}: {e}. "
                    f"Falling back to Settings defaults."
                )
                return {}
            return self._data

        if self._mtime == mtime and self._data is not None:
            return self._data

        with self._lock:
            if self._mtime == mtime and self._data is not None:
                return self._data

            try:
                with open(self._path, "r") as f:
                    parsed = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logger.error(
                    f"Failed to load trading params from {self._path}: {e}. "
                    f"{'Keeping previous copy' if self._data else 'Using Settings defaults'}."
                )
                return self._data or {}

            self._data = parsed
            self._mtime = mtime
            logger.info(f"Loaded trading params from {self._path} (mtime={mtime})")
            return self._data


_loader: TradingParamsLoader | None = None


def get_loader() -> TradingParamsLoader:
    global _loader
    if _loader is None:
        _loader = TradingParamsLoader()
    return _loader


def _normalize_regime(regime: str | None) -> str:
    if regime and regime.lower() in VALID_REGIMES:
        return regime.lower()
    return DEFAULT_REGIME


def _fallback_tier(config: Settings) -> dict[str, float]:
    """Build a tier dict from legacy Settings knobs."""
    return {
        "take_profit_pct": float(config.BOT_PRICE_THRESHOLD),
        "stop_loss_pct": float(config.BOT_MAX_STOP_LOSS_PCT),
        "leverage": float(config.BOT_LEVERAGE),
        "risk_per_trade": float(config.BOT_RISK_PER_TRADE),
    }


def _pick_tier(tiers: list[dict[str, Any]], balance: float) -> dict[str, Any] | None:
    for tier in tiers:
        lo = tier.get("min_balance")
        hi = tier.get("max_balance")
        if lo is not None and balance < lo:
            continue
        if hi is not None and balance > hi:
            continue
        return tier
    return None


def resolve_params(regime: str | None, balance: float) -> dict[str, float]:
    """Return trading params for the given regime + balance as decimals.

    The JSON stores percentages (e.g. ``2.6`` for 2.6%); this function
    converts them to decimal form (``0.026``) so callers can use the values
    directly as multipliers.

    Keys returned: ``take_profit_pct``, ``stop_loss_pct``, ``leverage``,
    ``risk_per_trade``, plus ``regime`` and ``source`` ("json" | "fallback").
    """
    config = get_settings()
    normalized = _normalize_regime(regime)
    data = get_loader().get()

    regimes = (data or {}).get("regimes", {})
    regime_block = regimes.get(normalized) or {}
    tiers = regime_block.get("balance_tiers") or []
    tier = _pick_tier(tiers, balance) or regime_block.get("default")

    if not tier:
        fallback = _fallback_tier(config)
        fallback["regime"] = normalized
        fallback["source"] = "fallback"
        return fallback

    try:
        return {
            "take_profit_pct": float(tier["take_profit_pct"]) / 100.0,
            "stop_loss_pct": float(tier["stop_loss_pct"]) / 100.0,
            "leverage": float(tier["leverage"]),
            "risk_per_trade": float(tier["risk_per_trade"]),
            "regime": normalized,
            "source": "json",
        }
    except (KeyError, TypeError, ValueError) as e:
        logger.error(
            f"Malformed trading params tier for regime={normalized} "
            f"balance={balance}: {e}. Falling back to Settings defaults."
        )
        fallback = _fallback_tier(config)
        fallback["regime"] = normalized
        fallback["source"] = "fallback"
        return fallback


def is_bear_trading_disabled() -> bool:
    """Top-level JSON switch: if true, new entries are blocked in bear regimes."""
    data = get_loader().get()
    return bool(data.get("disable_bear_trading", False)) if data else False
