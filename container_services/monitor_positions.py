import asyncio
import hashlib
import hmac
import json
import time
from datetime import datetime
from typing import Dict, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

from core.config import Settings, get_settings
from core.enums import OrderType
from core.logger import logger
from services.db_service import DbService
from services.third_party.bybit_api import BybitAPI
from services.third_party.telegram_api import TelegramNotifier
from services.trading_service import lookup_and_close_expired_positions
from utils.utils import (
    format_crypto_symbol,
    calculate_closed_position_stats,
    seconds_to_days_hours,
    build_stats_from_executions,
    build_stats_from_ws_payload,
)

config = get_settings()
telegram_notifier = TelegramNotifier(logger=logger)


class BybitWebSocketMonitor:
    """
    Enhanced real-time WebSocket monitor for Bybit with improved reliability
    """

    def __init__(self, _config: Settings, testnet: bool = False):
        self.api_key = _config.BYBIT_API_KEY
        self.api_secret = _config.BYBIT_API_SECRET
        self.testnet = testnet

        self.api_client = BybitAPI(_config)
        self.db = DbService(_config)

        # WebSocket URLs
        if testnet:
            self.ws_url = "wss://stream-testnet.bybit.com/v5/private"
        else:
            self.ws_url = "wss://stream.bybit.com/v5/private"

        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False

        # Callbacks
        self.position_closure_callbacks: list[Callable] = []
        self.position_opening_callbacks: list[Callable] = []

        # Position tracking
        self.tracked_positions: Dict[str, dict] = {}

        # Connection management
        self.reconnect_interval = 5  # Start with 5 seconds
        self.max_reconnect_interval = 60  # Max 60 seconds
        self.ping_interval = 20
        self.ping_timeout = 10
        self.last_ping = 0
        self.last_pong = 0
        self.connection_attempts = 0
        self.max_connection_attempts = 10

        # Message handling
        self.message_timeout = 30
        self.heartbeat_task: Optional[asyncio.Task] = None

    def add_position_closure_callback(self, callback: Callable):
        """Add callback function to execute when position closes"""
        self.position_closure_callbacks.append(callback)

    def add_position_opening_callback(self, callback: Callable):
        """Add callback function to execute when position opens"""
        self.position_opening_callbacks.append(callback)

    def _generate_auth_signature(self, expires: int) -> str:
        """Generate authentication signature for WebSocket"""
        param_str = f"GET/realtime{expires}"
        return hmac.new(
            bytes(self.api_secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _create_auth_message(self) -> dict:
        """Create authentication message"""
        expires = int((time.time() + 10) * 1000)
        signature = self._generate_auth_signature(expires)

        return {
            "op": "auth",
            "args": [self.api_key, expires, signature]
        }

    def _create_subscription_message(self, topics: list) -> dict:
        """Create subscription message"""
        return {
            "op": "subscribe",
            "args": topics
        }

    def _create_ping_message(self) -> dict:
        """Create ping message"""
        return {
            "op": "ping"
        }

    async def _send_message(self, message: dict) -> bool:
        """Safely send message to WebSocket"""
        if not self.websocket:
            return False

        try:
            await self.websocket.send(json.dumps(message))
            return True
        except ConnectionClosed:
            logger.warning("WebSocket connection closed while sending message")
            self.websocket = None
        except Exception as e:
            logger.error(f"Failed to send WebSocket message: {e}")
            # Connection might be dead, set to None
            self.websocket = None
        return False

    async def _handle_message(self, message: dict):
        """Handle incoming WebSocket messages"""
        try:
            op = message.get("op")

            # Handle authentication response
            if op == "auth":
                if message.get("success"):
                    logger.info("WebSocket authentication successful")
                    await self._subscribe_to_streams()
                    self.connection_attempts = 0  # Reset on successful auth
                else:
                    logger.error(f"WebSocket authentication failed: {message}")
                    raise Exception("Authentication failed")

            # Handle subscription response
            elif op == "subscribe":
                if message.get("success"):
                    logger.info(f"Successfully subscribed to topics")
                else:
                    logger.error(f"Subscription failed: {message}")

            # Handle position updates
            elif message.get("topic") == "position.linear":
                await self._handle_position_update(message.get("data", []))

            # Handle pong response
            elif op == "pong":
                self.last_pong = time.time()
                logger.debug("Received pong from server")

            # Handle other message types
            else:
                logger.debug(f"Received message: {message.get('topic', op)}")

        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")

    async def _handle_position_update(self, data: list):
        """Handle position update messages"""
        for position in data:
            try:
                symbol = position.get("symbol")
                size = float(position.get("size", 0))

                if not symbol:
                    continue

                logger.debug(f"Position update for {symbol}: size={size}")

                # Check if this is a new position opening
                if symbol not in self.tracked_positions and size != 0:
                    logger.info(f"New position opened: {symbol} size={size}")

                    # Execute opening callbacks
                    for callback in self.position_opening_callbacks:
                        try:
                            await callback(self.db, self.api_client, self.tracked_positions, position)
                        except Exception as e:
                            logger.error(f"Error in position opening callback for {symbol}: {e}")

                # Check for position closure
                elif symbol in self.tracked_positions:
                    old_size = float(self.tracked_positions[symbol].get("size", 0))

                    if old_size != 0 and size == 0:
                        logger.info(f"Position closed: {symbol}")

                        # Execute closure callbacks
                        for callback in self.position_closure_callbacks:
                            try:
                                await callback(self.db, self.api_client, self.tracked_positions, position)
                            except Exception as e:
                                logger.error(f"Error in position closure callback for {symbol}: {e}")

                    elif old_size != 0 and abs(size) > abs(old_size):
                        size_increase = abs(size) - abs(old_size)
                        logger.info(f"Position size increased for {symbol}: +{size_increase}")

                        for callback in self.position_opening_callbacks:
                            try:
                                await callback(self.db, self.api_client, self.tracked_positions, position)
                            except Exception as e:
                                logger.error(f"Error in position opening callback for {symbol}: {e}")

                # Update tracked position
                if size != 0:
                    self.tracked_positions[symbol] = position
                else:
                    self.tracked_positions.pop(symbol, None)

            except Exception as e:
                logger.error(f"Error processing position update: {e}")

    async def _subscribe_to_streams(self):
        """Subscribe to WebSocket streams"""
        topics = [
            "position.linear",  # Position updates
        ]

        subscription_msg = self._create_subscription_message(topics)
        success = await self._send_message(subscription_msg)

        if success:
            logger.info(f"Subscribed to topics: {topics}")
        else:
            raise Exception("Failed to subscribe to topics")

    async def _heartbeat_loop(self):
        """Maintain connection with periodic pings"""
        while self.is_running:
            try:
                current_time = time.time()

                # Send ping if needed
                if current_time - self.last_ping > self.ping_interval:
                    ping_msg = self._create_ping_message()
                    if await self._send_message(ping_msg):
                        self.last_ping = current_time
                        logger.debug("Sent ping to server")
                    else:
                        logger.warning("Failed to send ping")
                        break

                # Check for pong timeout
                if (self.last_pong > 0 and
                        current_time - self.last_pong > self.ping_timeout + self.ping_interval):
                    logger.warning("Pong timeout - connection may be dead")
                    break

                await asyncio.sleep(5)  # Check every 5 seconds

            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                break

    async def _connection_loop(self):
        """Main WebSocket connection loop with improved error handling"""
        while self.is_running and self.connection_attempts < self.max_connection_attempts:
            try:
                self.connection_attempts += 1
                logger.info(f"Connecting to WebSocket: {self.ws_url} (attempt {self.connection_attempts})")

                # Connection timeout
                async with websockets.connect(
                        self.ws_url,
                        ping_interval=None,  # We handle pings manually
                        ping_timeout=None,
                        close_timeout=10,
                        max_size=2 ** 20,  # 1MB max message size
                ) as websocket:
                    self.websocket = websocket
                    self.last_ping = time.time()
                    self.last_pong = time.time()

                    logger.info("WebSocket connected successfully")

                    # Start heartbeat task
                    self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                    # Authenticate
                    auth_msg = self._create_auth_message()
                    if not await self._send_message(auth_msg):
                        raise Exception("Failed to send authentication message")

                    # Message handling loop
                    while self.is_running:
                        try:
                            # Wait for message with timeout
                            message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=self.message_timeout
                            )

                            data = json.loads(message)
                            await self._handle_message(data)

                        except asyncio.TimeoutError:
                            logger.warning("WebSocket receive timeout")
                            break
                        except ConnectionClosed as e:
                            logger.warning(f"WebSocket connection closed: {e}")
                            break
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to decode WebSocket message: {e}")
                            continue
                        except Exception as e:
                            logger.error(f"Error in message loop: {e}")
                            break

            except InvalidStatusCode as e:
                logger.error(f"WebSocket connection failed with status {e.status_code}")
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
            finally:
                # Cleanup
                if self.heartbeat_task:
                    self.heartbeat_task.cancel()
                    try:
                        await self.heartbeat_task
                    except asyncio.CancelledError:
                        pass
                self.websocket = None

            if self.is_running and self.connection_attempts < self.max_connection_attempts:
                # Exponential backoff
                sleep_time = min(self.reconnect_interval * (2 ** min(self.connection_attempts - 1, 4)),
                                 self.max_reconnect_interval)
                logger.info(f"Reconnecting in {sleep_time} seconds...")
                await asyncio.sleep(sleep_time)

        if self.connection_attempts >= self.max_connection_attempts:
            logger.error(f"Max connection attempts ({self.max_connection_attempts}) reached. Stopping.")
            self.is_running = False

    async def start_monitoring_async(self, symbols: list[str] = None):
        """Start WebSocket monitoring asynchronously"""
        self.is_running = True
        self.connection_attempts = 0
        logger.info("Starting WebSocket monitoring...")

        await self._connection_loop()

    def stop_monitoring(self):
        """Stop WebSocket monitoring"""
        self.is_running = False
        logger.info("Stopping WebSocket monitoring...")


async def complete_position_opening(
        db: DbService,
        api_client: BybitAPI,
        order: dict,
        symbol: str,
        position: dict,
        position_size: float,
        trading_params: dict,
) -> None:
    # Idempotency: ensure we only run the side effects (Telegram notification,
    # set_trading_stop, record_position_event) once per (symbol, order.created_at).
    created_at = order.get("created_at")
    if created_at is None:
        logger.error(f"{symbol}: complete_position_opening missing order.created_at, skipping")
        return

    claimed = await db.claim_position_opened(symbol, created_at)
    if not claimed:
        logger.warning(
            f"{symbol}: complete_position_opening duplicate invocation detected "
            f"(order created_at={created_at}), skipping side effects"
        )
        return

    fresh_order = await db.get_active_order_by_symbol(symbol)
    if fresh_order and fresh_order.get("is_filled"):
        logger.warning(
            f"{symbol}: complete_position_opening called but order already is_filled=True, "
            f"skipping duplicate notification"
        )
        return

    average_price = float(position["avgPrice"])
    stop_loss = order["stop_loss"]
    order["average_price"] = average_price
    order["is_filled"] = True

    precision = int(trading_params["priceScale"])

    # Pull TP / SL / leverage from the snapshot captured at order open time
    # so the fill is sized and stopped with the same params that sized the
    # entry. Legacy orders without a snapshot fall back to Settings defaults.
    threshold = float(order.get("take_profit_pct") or config.BOT_PRICE_THRESHOLD)
    max_sl_pct = float(order.get("stop_loss_pct") or config.BOT_MAX_STOP_LOSS_PCT)
    snapshot_leverage = order.get("leverage")
    if snapshot_leverage:
        leverage = min(trading_params["leverage"], float(snapshot_leverage))
    else:
        leverage = min(trading_params["leverage"], config.BOT_LEVERAGE)

    # Calculate take profit and stop loss
    if order["order_type"] == OrderType.LONG:
        coeff = 1 + threshold
        if not stop_loss or abs(1 - average_price / stop_loss) > max_sl_pct:
            stop_loss = average_price * (1 - max_sl_pct)
    else:
        coeff = 1 - threshold
        if not stop_loss or abs(1 - stop_loss / average_price) > max_sl_pct:
            stop_loss = average_price * (1 + max_sl_pct)

    tp_price = average_price * coeff
    order["stop_loss"] = stop_loss

    # Update order in database
    await db.update_active_order_by_symbol(symbol, order)

    # Set trading stop
    await api_client.set_trading_stop(
        f"{symbol}USDT",
        take_profit=round(tp_price, precision),
        stop_loss=round(stop_loss, precision),
        tp_trigger_by="MarkPrice",
        sl_trigger_by="LastPrice",
    )

    active_orders = await db.get_active_orders()
    open_positions_str = f" ({', '.join(active_orders)})" if active_orders else ""
    position_ttl = datetime.fromtimestamp(order['created_at'] + config.BOT_MARKET_ORDERS_TTL * 86400)

    # Send Telegram notification
    trade_message = f"""✅ <strong>POSITION OPENED</strong>

    🪙 <strong>Symbol:</strong> ${symbol}USDT
    {"📈" if order["order_type"] == "LONG" else "📉"} <strong>Side:</strong> {order["order_type"]}
    ⚖️ <strong>Size:</strong> {position_size:,.3f}
    💎 <strong>Total Value:</strong> ${abs(position_size) * average_price / leverage:,.2f}\n
    🏷️ <strong>Average Price:</strong> ${average_price:,.{precision}f}
    🎯 <strong>Take Profit:</strong> ${average_price * coeff:,.{precision}f}
    🔴 <strong>Stop Loss:</strong> ${stop_loss:,.{precision}f}\n
    🕐 <strong>Active till:</strong> {position_ttl.strftime('%B %d, %Y - %H:%M')}\n
    📋 <strong>Open Positions:</strong> {len(active_orders)}{open_positions_str}"""

    await telegram_notifier.send_trade_update(trade_message)

    await db.record_position_event({
        "event_type": "opened",
        "symbol": symbol,
        "order_type": order["order_type"],
        "quantity": position_size,
        "average_price": average_price,
        "stop_loss": stop_loss,
        "timestamp": order["created_at"],
    })


# Callback functions
async def on_position_opened(db: DbService, api_client: BybitAPI, tracked_positions: dict, opening: dict):
    symbol = format_crypto_symbol(opening.get("symbol"))
    full_symbol = f"{symbol}USDT"

    logger.info(f"Position Opened - {opening}")
    try:
        order = await db.get_active_order_by_symbol(symbol)
        if not order:
            return

        is_filled = order.get("is_filled", False)
        if is_filled:
            msg = f"{symbol} has already open position"
            await telegram_notifier.send_service_notification(msg)
            logger.info(msg)
            return

        # Retry logic to wait for position to be fully filled
        max_tries = config.BOT_POSITION_REFRESH_ATTEMPTS
        position = None
        position_size = 0
        order_quantity = float(order.get("quantity", 0))

        for attempt in range(max_tries):
            position = await api_client.get_position_by_symbol(full_symbol)
            if not position:
                logger.error(f"Could not retrieve position for {symbol}USDT on attempt {attempt + 1}")
                if attempt < max_tries - 1:  # Don't sleep on the last attempt
                    await asyncio.sleep(config.BOT_POSITION_REFRESH_ATTEMPTS_DELAY)

                continue

            position_size = float(position.get("size", 0))

            # Check if position is fully filled
            if abs(position_size) == abs(order_quantity):
                logger.info(f"Position fully filled for {symbol} after {attempt + 1} attempts")
                break

            logger.info(
                f"Position not fully filled for {symbol}: {position_size}/{order_quantity} (attempt {attempt + 1})")

            if attempt < max_tries - 1:  # Don't sleep after the last attempt
                await asyncio.sleep(config.BOT_POSITION_REFRESH_ATTEMPTS_DELAY)

        if not position:
            logger.error(f"Could not retrieve position for {symbol}USDT after {max_tries} attempts")
            return

        trading_params = (await api_client.get_trading_params())[symbol]

        if abs(position_size) == abs(order_quantity):
            return await complete_position_opening(db, api_client, order, symbol, position, position_size, trading_params)

        elif (
                symbol in tracked_positions
                and float(tracked_positions[symbol].get("size", 0)) < position_size
                and not is_filled
        ) or (
                symbol not in tracked_positions and not is_filled
        ):
            order = await db.get_active_order_by_symbol(symbol)
            max_attempts = config.BOT_FILL_POSITION_RETRIES

            if order["attempt"] <= max_attempts:
                diff = order_quantity - position_size
                price = await api_client.get_current_price(full_symbol)
                price = price["mark_price"] or price["last_price"]

                if trading_params["minOrderQty"] >= diff or trading_params["minNotionalValue"] >= diff * price:
                    order["quantity"] = position_size
                    return await complete_position_opening(
                        db, api_client, order, symbol, position, position_size, trading_params,
                    )


                msg = (
                    f"{symbol} position partially filled after {order['attempt']}/{max_attempts} attempts: "
                    f"{position_size}/{order_quantity}. Trying again..."
                )
                await telegram_notifier.send_service_notification(msg)
                logger.info(msg)

                order["attempt"] += 1
                await db.update_active_order_by_symbol(symbol, order)

                side = "Buy" if order["order_type"] == OrderType.LONG else "Sell"
                await api_client.open_position_with_market_orders(full_symbol, side, diff)
            else:
                msg = (
                    f"Failed to fill {symbol} position after {max_attempts} attempts. "
                    f"Leaving the position partially filled: {position_size}/{order_quantity}"
                )

                await telegram_notifier.send_service_notification(msg)
                logger.info(msg)

                order["quantity"] = position_size
                return await complete_position_opening(
                    db, api_client, order, symbol, position, position_size, trading_params,
                )

    except Exception as e:
        logger.error(f"Error in position opened callback for {symbol}: {e}")


async def _try_closed_pnl_stats(
        api_client: BybitAPI,
        symbol: str,
        order: dict,
        leverage: float,
        max_tries: int,
) -> Optional[dict]:
    """Retry-fetch closed-pnl stats. Returns None if unavailable after all attempts."""
    order_quantity = float(order.get("quantity", 0))
    order_value = order.get("quantity", 0) * order.get("average_price", 0) / leverage
    stats: Optional[dict] = None

    for attempt in range(max_tries):
        try:
            closed_positions = await api_client.get_most_recent_closed_position(symbol)
            stats = await calculate_closed_position_stats(closed_positions, order_value)

            if not stats:
                logger.warning(f"No stats calculated for {symbol} on attempt {attempt + 1}")
                if attempt < max_tries - 1:
                    await asyncio.sleep(config.BOT_POSITION_REFRESH_ATTEMPTS_DELAY)
                continue

            total_quantity = stats.get("total_quantity", 0)

            if abs(total_quantity) == abs(order_quantity):
                logger.info(f"Closed position data fully available for {symbol} after {attempt + 1} attempts")
                return stats

            logger.info(
                f"Closed position data incomplete for {symbol}: "
                f"{total_quantity}/{order_quantity} (attempt {attempt + 1})"
            )

            if attempt < max_tries - 1:
                await asyncio.sleep(config.BOT_POSITION_REFRESH_ATTEMPTS_DELAY)

        except Exception as e:
            logger.warning(f"Error getting closed position data for {symbol} on attempt {attempt + 1}: {e}")
            if attempt < max_tries - 1:
                await asyncio.sleep(config.BOT_POSITION_REFRESH_ATTEMPTS_DELAY)
            continue

    if stats and abs(stats.get("total_quantity", 0)) != abs(order_quantity):
        logger.warning(
            f"Closed position data still incomplete for {symbol} after {max_tries} attempts: "
            f"{stats.get('total_quantity', 0)}/{order_quantity}. Using partial data."
        )
        return stats

    return None


async def _try_executions_stats(
        api_client: BybitAPI,
        symbol: str,
        order: dict,
        leverage: float,
) -> Optional[dict]:
    """Fallback: reconstruct stats from /v5/execution/list."""
    try:
        created_at = order.get("created_at")
        start_time_ms = int(float(created_at) * 1000) if created_at else None
        executions = await api_client.get_executions(
            symbol=f"{symbol}USDT",
            start_time=start_time_ms,
        )
        stats = build_stats_from_executions(executions, order, leverage)
        if stats:
            logger.info(f"Reconstructed close stats for {symbol} from executions")
        return stats
    except Exception as e:
        logger.warning(f"Executions fallback failed for {symbol}: {e}")
        return None


def _format_amount(value: Optional[float], prefix: str = "$", fmt: str = ",.2f") -> str:
    if value is None:
        return "N/A"
    return f"{prefix}{value:{fmt}}"


def _format_close_message(
        symbol: str,
        order: dict,
        stats: dict,
        stats_source: str,
        precision: int,
        balance: float,
        active_orders: list,
        duration_seconds: float,
) -> str:
    pnl = stats.get("pnl")
    pnl_pct = stats.get("pnl_pct")
    total_fee = stats.get("total_fee")
    exit_price = stats.get("average_exit_price")
    total_quantity = stats.get("total_quantity", 0) or 0

    if pnl is None:
        pnl_line = "🟡 <strong>P&L:</strong> N/A (stats unavailable)"
    else:
        pnl_emoji = "🟢" if pnl >= 0 else "🔴"
        pnl_pct_str = f"{pnl_pct:.2f}%" if pnl_pct is not None else "N/A"
        pnl_line = f"{pnl_emoji} <strong>P&L:</strong> {pnl_pct_str} (${pnl:,.2f})"

    entry_price = order.get("average_price")
    entry_str = f"${round(entry_price, precision):,.{precision}f}" if entry_price else "N/A"
    exit_str = f"${round(exit_price, precision):,.{precision}f}" if exit_price else "N/A"

    fee_str = _format_amount(total_fee)

    open_positions_str = f" ({', '.join(active_orders)})" if active_orders else ""

    source_footnotes = {
        "closed_pnl": "",
        "executions": "\nℹ️ <em>Stats source: executions fallback</em>",
        "ws_payload": "\nℹ️ <em>Stats source: WebSocket payload (fees unavailable)</em>",
        "degraded": "\n⚠️ <em>Stats unavailable — please verify on Bybit</em>",
    }
    source_line = source_footnotes.get(stats_source, "")

    return f"""🏆 <strong>POSITION CLOSED</strong>

🪙 <strong>Symbol:</strong> ${symbol}USDT
{"📈" if order.get("order_type") == "LONG" else "📉"} <strong>Side:</strong> {order.get("order_type", "N/A")}
⚖️ <strong>Size:</strong> {total_quantity:,.3f}
{pnl_line}\n
👛 <strong>Balance:</strong> ${balance:,.2f}
🏷️ <strong>Fee:</strong> {fee_str}
📥 <strong>Entry Price:</strong> {entry_str}
📤 <strong>Exit Price:</strong> {exit_str}\n
🕐 <strong>Duration:</strong> {seconds_to_days_hours(duration_seconds)}
📋 <strong>Open Positions:</strong> {len(active_orders)}{open_positions_str}{source_line}"""


async def on_position_closed(db: DbService, api_client: BybitAPI, tracked_positions: dict, closure: dict):
    symbol = format_crypto_symbol(closure.get("symbol"))
    logger.info(f"Position Closed - {closure}")

    # Capture pre-close tracked state before _handle_position_update pops it.
    old_tracked = dict(tracked_positions.get(symbol, {}))

    try:
        order = await db.get_active_order_by_symbol(symbol)
        if not order:
            logger.warning(f"No active order found for closed position {symbol}")
            return

        # Get precision info
        trading_params = await api_client.get_trading_params()
        precision = int(trading_params[symbol]["priceScale"])
        # Prefer the per-order leverage snapshot so PnL math matches the
        # params used at entry. Older orders without a snapshot fall back
        # to Settings defaults.
        snapshot_leverage = order.get("leverage")
        if snapshot_leverage:
            leverage = min(trading_params[symbol]["leverage"], float(snapshot_leverage))
        else:
            leverage = min(trading_params[symbol]["leverage"], config.BOT_LEVERAGE)

        order_quantity = float(order.get("quantity", 0))
        max_tries = config.BOT_POSITION_REFRESH_ATTEMPTS * 2

        # 1. Primary path: closed-pnl with existing retry loop.
        stats = await _try_closed_pnl_stats(api_client, symbol, order, leverage, max_tries)
        stats_source = "closed_pnl" if stats else None

        # 2. Fallback: reconstruct from executions.
        if not stats:
            stats = await _try_executions_stats(api_client, symbol, order, leverage)
            if stats:
                stats_source = "executions"

        # 3. Fallback: derive from the WS closure payload itself.
        if not stats:
            stats = build_stats_from_ws_payload(closure, old_tracked, order, leverage)
            if stats:
                stats_source = "ws_payload"
                logger.info(f"Built close stats for {symbol} from WebSocket payload")

        # 4. Degraded: no numbers at all, but we still clean up + notify.
        if not stats:
            logger.error(
                f"Could not get closed position stats for {symbol} after "
                f"{max_tries} closed-pnl attempts + executions + ws fallbacks. "
                f"Proceeding with DB cleanup and degraded notification."
            )
            fallback_qty = 0.0
            try:
                fallback_qty = abs(float(old_tracked.get("size") or order_quantity or 0))
            except (ValueError, TypeError):
                fallback_qty = abs(order_quantity)
            stats = {
                "total_quantity": fallback_qty,
                "average_exit_price": None,
                "pnl": None,
                "pnl_pct": None,
                "total_fee": None,
            }
            stats_source = "degraded"

        # Record position event (best-effort fields only).
        try:
            await db.record_position_event({
                "event_type": "closed",
                "symbol": symbol,
                "order_type": order.get("order_type"),
                "quantity": stats.get("total_quantity"),
                "average_price": order.get("average_price"),
                "exit_price": stats.get("average_exit_price"),
                "stop_loss": order.get("stop_loss"),
                "pnl": stats.get("pnl"),
                "pnl_pct": stats.get("pnl_pct"),
                "total_fee": stats.get("total_fee"),
                "stats_source": stats_source,
                "timestamp": time.time(),
                "duration_seconds": time.time() - order["created_at"],
            })
        except Exception as e:
            logger.error(f"Failed to record position event for {symbol}: {e}")

        # Always clean up the active order — the WS event confirmed the close.
        try:
            await db.remove_active_order_by_symbol(symbol)
            await db.remove_active_order(symbol)
            logger.info(f"Removed active order for closed position {symbol}")
        except Exception as e:
            logger.error(f"Failed to remove active order for {symbol}: {e}")

        # Always send closure notification.
        try:
            balance = (await api_client.get_usdt_balance())["total_usdt"]
        except Exception as e:
            logger.warning(f"Could not fetch balance for close notification of {symbol}: {e}")
            balance = 0.0

        try:
            active_orders = await db.get_active_orders()
        except Exception as e:
            logger.warning(f"Could not fetch active orders for close notification of {symbol}: {e}")
            active_orders = []

        try:
            duration_seconds = time.time() - float(order["created_at"])
        except (KeyError, TypeError, ValueError):
            duration_seconds = 0.0

        closure_message = _format_close_message(
            symbol=symbol,
            order=order,
            stats=stats,
            stats_source=stats_source,
            precision=precision,
            balance=balance,
            active_orders=active_orders,
            duration_seconds=duration_seconds,
        )

        await telegram_notifier.send_trade_update(closure_message)

    except Exception as e:
        logger.error(f"Error in position closed callback for {symbol}: {e}")


async def _expiry_check_loop(interval: int):
    """Periodically close positions that exceeded BOT_MARKET_ORDERS_TTL."""
    while True:
        try:
            await lookup_and_close_expired_positions()
        except Exception as e:
            logger.error(f"Error in expiry check loop: {e}")
        await asyncio.sleep(interval)


async def _ma_cross_check_loop(api_client: BybitAPI, db: DbService, interval: int):
    """Every `interval` seconds, compute BTC 50/100-day SMA cross on closed
    daily candles and publish the active market regime to Redis:
    - SMA50 < SMA100 -> "bear"
    - SMA50 >= SMA100 -> "bull"

    The regime drives dynamic TP / SL / leverage / risk-per-trade lookup
    against bot/core/trading_params.json. Only writes (and sends a Telegram
    notification) when the regime flips.
    """
    long_w = config.BOT_MA_CROSS_LONG_WINDOW
    short_w = config.BOT_MA_CROSS_SHORT_WINDOW
    # Need `long_w` CLOSED candles + 1 in-progress candle we drop.
    fetch_limit = long_w + 1

    while True:
        try:
            candles = await api_client.get_kline(
                symbol=config.BOT_MA_CROSS_SYMBOL,
                interval="D",
                limit=fetch_limit,
            )
            # Bybit returns newest-first; drop in-progress candle at index 0.
            closed = candles[1 : 1 + long_w]

            if len(closed) < long_w:
                logger.warning(
                    f"MA cross: only got {len(closed)} closed candles, "
                    f"need {long_w}. Skipping this tick."
                )
            else:
                closes = [c["close"] for c in closed]
                sma_short = sum(closes[:short_w]) / short_w
                sma_long = sum(closes) / long_w

                regime = "bear" if sma_short < sma_long else "bull"
                current = await db.get_market_regime()

                logger.info(
                    f"BTC MA cross: SMA{short_w}={sma_short:.2f} "
                    f"SMA{long_w}={sma_long:.2f} -> regime={regime}"
                )

                if current != regime:
                    await db.set_market_regime(regime)
                    label = (
                        f"BEAR (SMA{short_w} &lt; SMA{long_w})"
                        if regime == "bear"
                        else f"BULL (SMA{short_w} &gt;= SMA{long_w})"
                    )
                    prev_str = current.upper() if current else "UNSET"
                    await telegram_notifier.send_trade_update(
                        f"📊 <strong>BTC MA regime flipped: {label}</strong>\n"
                        f"SMA{short_w}: {sma_short:,.2f}\n"
                        f"SMA{long_w}: {sma_long:,.2f}\n"
                        f"Regime: {prev_str} → {regime.upper()}"
                    )
        except Exception as e:
            logger.error(f"Error in MA cross check loop: {e}")
        await asyncio.sleep(interval)


async def main():
    """Main function to run the WebSocket monitor"""
    try:
        config = get_settings()
        monitor = BybitWebSocketMonitor(config, testnet=False)

        monitor.add_position_opening_callback(on_position_opened)
        monitor.add_position_closure_callback(on_position_closed)

        logger.info("Starting Bybit WebSocket monitor...")
        await asyncio.gather(
            monitor.start_monitoring_async(),
            _expiry_check_loop(config.BOT_EXPIRY_CHECK_INTERVAL_SECONDS),
            _ma_cross_check_loop(
                monitor.api_client,
                monitor.db,
                config.BOT_MA_CROSS_CHECK_INTERVAL_SECONDS,
            ),
        )

    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        if 'monitor' in locals():
            monitor.stop_monitoring()

        logger.info("Monitor stopped")


if __name__ == "__main__":
    asyncio.run(main())
