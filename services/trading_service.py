import asyncio
import json
import math
import time

from core.config import get_settings
from core.enums import EntryType, OrderType
from core.logger import logger
from core.trading_params import (
    DEFAULT_REGIME,
    is_bear_trading_disabled,
    resolve_params,
)
from models.openai_models import Message
from services.db_service import DbService
from services.third_party.bybit_api import BybitAPI
from services.third_party.telegram_api import TelegramNotifier
from utils.utils import round_by_min_order_size

# Initialize global services and configurations
config = get_settings()  # Load application settings
db = DbService(config)  # Initialize database service
bybit_api = BybitAPI(config)  # Initialize Bybit API client
telegram_notifier = TelegramNotifier(logger=logger)  # Initialize Telegram notification service


async def handle_close_order_action(symbol: str, parsed_message: Message):
    """
    Handle CLOSE_ORDER and TAKE_PROFIT actions

    Args:
        symbol: Trading pair symbol (e.g., 'BTC')
        parsed_message: Parsed message containing order details
    """

    # Get list of currently active orders
    active_orders = await db.get_active_orders()

    if symbol in active_orders:
        await bybit_api.close_position_with_market_orders(f"{symbol}USDT")
        logger.info(f"Handling close order action for {symbol}")
        return

    # No active position - fall back to checking for a pending limit order
    if await db.has_limit_order(symbol):
        await db.remove_symbol_from_monitor(symbol)
        await db.remove_limit_order(symbol)
        msg = f"Cancelled pending limit order for {symbol}"
        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)
        return

    msg = f"{symbol} not found in list of active orders"
    await telegram_notifier.send_service_notification(
        f"{msg}\n\n<code>{json.dumps(parsed_message.model_dump(), indent=4)}</code>"
    )
    logger.info(msg)


async def __set_leverage_no_exc(full_symbol: str, leverage: float):
    try:
        await bybit_api.set_leverage(full_symbol, leverage)
    except Exception:
        pass


async def execute_market_order(
        symbol: str,
        stop_loss: float,
        order_type: str,
        position_size_usdt: float,
        trading_params: dict | None = None,
        market_depth: dict | None = None,
        tier: dict | None = None,
):
    full_symbol = f"{symbol}USDT"

    # Idempotency: refuse to open a second position if one is already active for this symbol.
    # Protects against bursts of callers (e.g. limit-trigger ticks in BybitPriceMonitor.handle_message)
    # that re-invoke execute_market_order before the first one finishes.
    existing_active = await db.get_active_orders()
    if symbol in existing_active:
        msg = f"{symbol}: skipping execute_market_order, symbol already has an active order"
        logger.warning(msg)
        await telegram_notifier.send_service_notification(msg)
        return

    if not trading_params:
        trading_params = (await bybit_api.get_trading_params())[symbol]

    # Calculate market depth to determine maximum order size with acceptable slippage
    if not market_depth:
        market_depth = await bybit_api.calculate_market_depth(full_symbol, config.BOT_SLIPPAGE_TOLERANCE)

    max_quantity = market_depth["ask_quantity"]  # Maximum quantity available at acceptable price

    # Resolve tier late-bind if caller didn't supply one (direct callers, tests, etc.).
    if tier is None:
        balance_info = await bybit_api.get_usdt_balance()
        regime = await db.get_market_regime() or DEFAULT_REGIME
        tier = resolve_params(regime, balance_info["total_usdt"])

    leverage = min(trading_params["leverage"], tier["leverage"])

    results = await asyncio.gather(
        bybit_api.get_current_price(full_symbol),
        __set_leverage_no_exc(full_symbol, leverage),
        return_exceptions=True,
    )

    # Check for exceptions
    for i in range(len(results)):
        if isinstance(results[i], Exception):
            raise results[i]

    price = results[0]["mark_price"] or results[0]["last_price"]
    quantity = min(
        max_quantity,
        round_by_min_order_size(position_size_usdt / price * leverage, trading_params["minOrderQty"]),
    )

    await db.create_active_order_by_symbol(symbol, {
        "stop_loss": stop_loss,
        "order_type": order_type,
        "quantity": quantity,
        "is_filled": False,
        "created_at": time.time(),
        "attempt": 1,
        # Snapshot of the resolved tier so later fill-time logic
        # (complete_position_opening, on_position_closed) computes TP/SL
        # and PnL with the same params that sized the entry.
        "take_profit_pct": tier["take_profit_pct"],
        "stop_loss_pct": tier["stop_loss_pct"],
        "leverage": leverage,
        "risk_per_trade": tier["risk_per_trade"],
        "regime": tier.get("regime", DEFAULT_REGIME),
    })
    await db.add_active_order(symbol)

    # Create a new active order with stop loss and order type
    side = "Buy" if order_type == OrderType.LONG else "Sell"
    await bybit_api.open_position_with_market_orders(full_symbol, side, quantity)

    logger.info(
        f"Opened the position in {symbol} using market price ({price:,} USDT). "
        f"Max amount {market_depth['ask_depth_usdt']:,.2f} USDT (quantity: {max_quantity}). "
        f"Position size: {position_size_usdt:,.2f} USDT ({quantity}). Leverage: {leverage:,.2f}"
    )


async def _execute_limit_order(
        symbol: str,
        entry_price: float,
        stop_loss: float,
        order_type: str,
        tier: dict | None = None,
):
    """
    Handle limit order creation (will execute when price reaches specified level)

    Args:
        symbol: Trading pair symbol
        entry_price: Price at which to execute the order
        stop_loss: Stop loss price level
        order_type: Type of order (LONG or SHORT)
        tier: Resolved trading params snapshot (TP / SL / leverage / risk)
    """

    # Snapshot the tier on the limit order so the trigger path (monitor_prices)
    # fires with the same params even if the regime flips while we wait.
    payload = {
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "order_type": order_type,
    }
    if tier:
        payload.update({
            "take_profit_pct": tier["take_profit_pct"],
            "stop_loss_pct": tier["stop_loss_pct"],
            "leverage": tier["leverage"],
            "risk_per_trade": tier["risk_per_trade"],
            "regime": tier.get("regime", DEFAULT_REGIME),
        })
    await db.create_limit_order(symbol, payload)

    # Add symbol to monitoring list to track when price reaches entry point
    await db.add_symbol_to_monitor(symbol)

    # Notify about the created limit order
    msg = f"Added {symbol} to limit orders await list. Entry price - {entry_price} USDT"
    await telegram_notifier.send_service_notification(msg)
    logger.info(msg)


async def calculate_locked_usdt(active_orders: list[str], trading_params: dict | None = None) -> float:
    if not active_orders:
        return 0

    if not trading_params:
        trading_params = await bybit_api.get_trading_params()

    orders = await db.get_active_orders_by_symbols(active_orders)

    locked_usdt = 0
    for symbol, order_info in orders.items():
        # Prefer the leverage snapshot stored on the order when it was opened.
        # Fall back to the exchange-capped Settings value for legacy orders
        # that predate the snapshot.
        snapshot_leverage = order_info.get("leverage")
        if snapshot_leverage:
            leverage = float(snapshot_leverage)
        else:
            leverage = min(trading_params[symbol]["leverage"], config.BOT_LEVERAGE)
        locked_usdt += math.floor(
            order_info.get("quantity", 0) * order_info.get("average_price", 0) / leverage
        )

    return locked_usdt


async def skip_the_signal_notification(symbol: str, parsed_message: Message, err: str | None = None) -> None:
    msg = f"{symbol} is not available on Bybit. Skipping the signal"
    if err:
        msg += f"\n\n<code>{err}</code>"

    await telegram_notifier.send_service_notification(
        f"{msg}\n\n<code>{json.dumps(parsed_message.model_dump(), indent=4)}</code>"
    )
    logger.info(msg)

    return


async def handle_new_trade_action(symbol: str, parsed_message: Message):
    """
    Handle CREATE_ORDER action - Process new trade signals

    Args:
        symbol: Trading pair symbol
        parsed_message: Parsed message containing order details
    """
    logger.info(f"Handling new trade action for {symbol}")
    logger.debug(f"Order details: {parsed_message}")

    # Check if the symbol is blacklisted (not allowed to trade)
    if symbol in config.BOT_BLACKLISTED_COINS:
        msg = f"{symbol} is blacklisted"
        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)
        return

    # Ensure stop loss is provided (required for risk management)
    if config.BOT_REQUIRE_STOP_LOSS and not parsed_message.stop_loss:
        msg = f"{symbol} got no stop loss value. Skipping the signal"
        await telegram_notifier.send_service_notification(
            f"{msg}\n\n<code>{json.dumps(parsed_message.model_dump(), indent=4)}</code>"
        )
        logger.info(msg)
        return

    results = await asyncio.gather(
        bybit_api.get_trading_params(),
        bybit_api.get_usdt_balance(),
        bybit_api.calculate_market_depth(f"{symbol}USDT", config.BOT_SLIPPAGE_TOLERANCE),
        return_exceptions=True,
    )

    # Check for exceptions
    for i in range(len(results)):
        if isinstance(results[i], Exception):
            return await skip_the_signal_notification(symbol, parsed_message, str(results[i]))

    trading_params = results[0]
    balance = results[1]["total_usdt"]
    market_depth = results[2]

    # Verify the symbol is available for trading on Bybit
    if symbol not in trading_params.keys():
        return await skip_the_signal_notification(symbol, parsed_message)

    # Set default values if not provided in the message
    entry_type = parsed_message.entry_type or EntryType.MARKET_PRICE
    order_type = parsed_message.order_type or OrderType.LONG
    stop_loss = float(parsed_message.stop_loss) if parsed_message.stop_loss else None

    # Check for existing active position with this symbol
    active_orders = await db.get_active_orders()
    if symbol in active_orders:
        msg = f"{symbol} has already active position. Skipping the signal"
        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)
        return

    # Resolve the regime-aware trading params for this balance tier.
    # Regime comes from the MA-cross loop (Redis); defaults to bull when unset.
    regime = await db.get_market_regime() or DEFAULT_REGIME
    if regime == "bear" and is_bear_trading_disabled():
        msg = (
            f"{symbol}: skipping signal — disable_bear_trading is ON "
            f"and current regime is BEAR"
        )
        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)
        return

    tier = resolve_params(regime, balance)

    leverage = min(trading_params[symbol]["leverage"], tier["leverage"])
    position_size_usdt = min(
        config.BOT_MAX_ORDER_USD,
        math.floor(balance * tier["risk_per_trade"]),
        math.floor(market_depth['ask_depth_usdt'] / leverage),
    )

    locked_usdt = await calculate_locked_usdt(active_orders, trading_params)
    if position_size_usdt + locked_usdt > balance * config.BOT_MAX_SIMULTANEOUS_BALANCE_USAGE:
        msg = (
            f"Cannot open new position for {symbol}. "
            f"The total balance usage exceeds {config.BOT_MAX_SIMULTANEOUS_BALANCE_USAGE * 100}%: "
            f"{(position_size_usdt + locked_usdt) / balance * 100}%"
        )

        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)
        return

    # Get list of symbols currently being monitored for limit orders
    symbols_to_monitor = await db.get_symbols_to_monitor()

    # If symbol was in monitoring list but not in active orders,
    # it means it was a limit order that we're now executing as market order
    if entry_type == EntryType.MARKET_PRICE and symbol not in active_orders and symbol in symbols_to_monitor:
        msg = f"Moving {symbol} from limit orders to market orders"
        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)

        # Remove the limit order and stop monitoring this symbol
        await db.remove_limit_order(symbol)
        await db.remove_symbol_from_monitor(symbol)

    # Check if this symbol is already in the limit orders monitoring list
    if symbol in symbols_to_monitor:
        msg = f"{symbol} is already in limit orders await list. Skipping the signal"
        await telegram_notifier.send_service_notification(msg)
        logger.info(msg)
        return

    # For limit orders, prepare the order with the specified entry price
    if entry_type == EntryType.LIMIT_ORDER:
        # Round the entry price to the appropriate precision for this symbol
        entry_price = round(float(parsed_message.entry_price), trading_params[symbol]["priceScale"])
        return await _execute_limit_order(symbol, entry_price, stop_loss, str(order_type), tier)

    # For market orders, execute immediately at current price
    if entry_type == EntryType.MARKET_PRICE:
        return await execute_market_order(
            symbol, stop_loss, str(order_type), position_size_usdt,
            trading_params[symbol], market_depth, tier,
        )


EXPIRED_CLOSE_CONCURRENCY = 3
EXPIRED_CLOSE_TIMEOUT_S = 30


async def _close_expired_position(symbol: str, order: dict) -> None:
    msg = f"{symbol}USD position is older than {config.BOT_MARKET_ORDERS_TTL} days. Closing the position"
    await telegram_notifier.send_service_notification(msg)
    logger.info(msg)

    await bybit_api.close_position_with_market_orders(f"{symbol}USDT")

    now = time.time()
    await db.record_position_event({
        "event_type": "closed",
        "symbol": symbol,
        "order_type": order.get("order_type"),
        "quantity": order.get("quantity", 0),
        "average_price": order.get("average_price", 0),
        "stop_loss": order.get("stop_loss"),
        "timestamp": now,
        "duration_seconds": now - order["created_at"],
        "expired": True,
    })

    await db.remove_active_order_by_symbol(symbol)
    await db.remove_active_order(symbol)


async def lookup_and_close_expired_positions():
    logger.debug(f"Looking for expired positions to close")

    active_orders = await db.get_active_orders()
    if not active_orders:
        return

    orders = await db.get_active_orders_by_symbols(active_orders)
    ttl_s = config.BOT_MARKET_ORDERS_TTL * 24 * 60 * 60
    now = time.time()
    expired = [(s, o) for s, o in orders.items() if now - o["created_at"] >= ttl_s]

    if not expired:
        return

    sem = asyncio.Semaphore(EXPIRED_CLOSE_CONCURRENCY)

    # Rate-limited fan-out: one slow Bybit call can't stall the rest,
    # and a single failure is logged without aborting siblings.
    async def _close(symbol: str, order: dict) -> None:
        async with sem:
            try:
                async with asyncio.timeout(EXPIRED_CLOSE_TIMEOUT_S):
                    await _close_expired_position(symbol, order)
            except Exception as e:
                logger.error(f"Failed to close expired {symbol}: {e}")

    await asyncio.gather(*(_close(s, o) for s, o in expired))
