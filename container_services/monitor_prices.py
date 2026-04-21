import asyncio
import json
import math

import websockets

from core.config import get_settings
from core.enums import OrderType
from core.logger import logger
from core.trading_params import (
    DEFAULT_REGIME,
    is_bear_trading_disabled,
    resolve_params,
)
from services.db_service import DbService
from services.third_party.bybit_api import BybitAPI
from services.third_party.telegram_api import TelegramNotifier
from services.trading_service import execute_market_order, config, calculate_locked_usdt
from utils.utils import format_crypto_symbol

telegram_notifier = TelegramNotifier(logger=logger)


class BybitPriceMonitor:
    def __init__(self, config):
        self.api_client = BybitAPI(config)
        self.db = DbService(config)

        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.websocket = None

        self.symbols = []
        self.limit_orders_data = {}
        self.running = False

    async def __remove_expired_limit_orders(self):
        symbols = list(self.symbols)
        if not symbols:
            return

        orders = await self.db.get_limit_orders_by_symbols(symbols)

        expired: list[str] = []
        for symbol in symbols:
            if order := orders.get(symbol.upper()):
                self.limit_orders_data[symbol] = order
            else:
                expired.append(symbol)

        if not expired:
            return

        for symbol in expired:
            logger.info(f"TTL expired. Removing {symbol} from limit orders to monitor")
        await self.db.remove_symbols_from_monitor(expired)

    async def connect(self):
        """Connect to Bybit WebSocket"""
        try:
            self.symbols = set(await self.db.get_symbols_to_monitor())
            await self.__remove_expired_limit_orders()
            self.symbols = set(await self.db.get_symbols_to_monitor())

            self.websocket = await websockets.connect(self.ws_url)
            logger.info(f"Connected to Bybit WebSocket: {self.ws_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False

    async def subscribe_to_tickers(self):
        """Subscribe to ticker data for specified symbols"""
        subscription_message = {
            "op": "subscribe",
            "args": [f"tickers.{symbol}USDT" for symbol in self.symbols]
        }

        try:
            await self.websocket.send(json.dumps(subscription_message))
            logger.info(f"Subscribed to tickers: {', '.join(self.symbols)}")
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")

    async def handle_message(self, message):
        """Process incoming WebSocket messages"""
        try:
            data = json.loads(message)

            # Handle subscription confirmation
            if data.get("success") and data.get("op") == "subscribe":
                logger.info("Subscription confirmed")
                return

            # Handle ticker data
            if data.get("topic", "").startswith("tickers."):
                trading_params = await self.api_client.get_trading_params()

                ticker_data = data.get("data", {})
                price = ticker_data.get("lastPrice")
                if price:
                    symbol = format_crypto_symbol(ticker_data.get("symbol", ""))
                    if symbol == config.BOT_DUMMY_MONITORING_SYMBOL:
                        logger.debug(f"It's {config.BOT_DUMMY_MONITORING_SYMBOL} price. Skipping")
                        return

                    precision = int(trading_params[symbol]["priceScale"])
                    current_price = round(float(price), precision)
                    logger.info(f"{symbol} - {price} USDT")

                    try:
                        limit_price = round(self.limit_orders_data[symbol]["entry_price"], precision)
                        order_type = self.limit_orders_data[symbol]["order_type"]
                    except KeyError:
                        logger.error(f"{symbol} not found in limit orders list")
                        return

                    if (
                            order_type == OrderType.LONG and current_price <= limit_price
                    ) or (
                            order_type == OrderType.SHORT and current_price >= limit_price
                    ):
                        logger.info(f"Executing the market order for {symbol}. "
                                    f"Removing it from limit orders monitoring list")

                        # Re-check regime + bear-disable at trigger time so a
                        # regime flip between limit creation and fill is honored.
                        regime = await self.db.get_market_regime() or DEFAULT_REGIME
                        if regime == "bear" and is_bear_trading_disabled():
                            msg = (
                                f"{symbol}: skipping triggered limit order — "
                                f"disable_bear_trading is ON and current regime is BEAR"
                            )
                            await telegram_notifier.send_service_notification(msg)
                            logger.info(msg)
                            await self.db.remove_limit_order(symbol)
                            await self.db.remove_symbol_from_monitor(symbol)
                            return

                        active_orders = await self.db.get_active_orders()
                        results = await asyncio.gather(
                            self.api_client.get_usdt_balance(),
                            self.api_client.calculate_market_depth(f"{symbol}USDT", config.BOT_SLIPPAGE_TOLERANCE),
                            return_exceptions=True,
                        )

                        # Check for exceptions
                        for i in range(len(results)):
                            if isinstance(results[i], Exception):
                                raise results[i]

                        balance = results[0]["total_usdt"]
                        market_depth = results[1]

                        # Prefer the tier snapshot stored on the limit order so
                        # params stay consistent with what the operator saw at
                        # creation time. If it's missing (legacy limit order),
                        # re-resolve from the current regime + balance.
                        stored = self.limit_orders_data[symbol]
                        if all(k in stored for k in ("take_profit_pct", "stop_loss_pct", "leverage", "risk_per_trade")):
                            tier = {
                                "take_profit_pct": float(stored["take_profit_pct"]),
                                "stop_loss_pct": float(stored["stop_loss_pct"]),
                                "leverage": float(stored["leverage"]),
                                "risk_per_trade": float(stored["risk_per_trade"]),
                                "regime": stored.get("regime", regime),
                                "source": "limit_snapshot",
                            }
                        else:
                            tier = resolve_params(regime, balance)

                        leverage = min(trading_params[symbol]["leverage"], tier["leverage"])

                        position_size_usdt = min(
                            config.BOT_MAX_ORDER_USD,
                            math.floor(balance * tier["risk_per_trade"]),
                            math.floor(market_depth['ask_depth_usdt'] / leverage),
                        )

                        locked_usdt = await calculate_locked_usdt(active_orders, trading_params)

                        if position_size_usdt + locked_usdt > balance * config.BOT_MAX_SIMULTANEOUS_BALANCE_USAGE:
                            msg = (f"Cannot open new position for {symbol}. "
                                   f"The total balance usage exceeds {config.BOT_MAX_SIMULTANEOUS_BALANCE_USAGE * 100}%: "
                                   f"{(position_size_usdt + locked_usdt) / balance * 100}%")

                            await telegram_notifier.send_service_notification(msg)
                            logger.info(msg)
                            return

                        await execute_market_order(
                            symbol, self.limit_orders_data[symbol]["stop_loss"],
                            order_type, position_size_usdt, trading_params[symbol], market_depth,
                            tier,
                        )

                        await self.db.remove_limit_order(symbol)
                        await self.db.remove_symbol_from_monitor(symbol)

                        logger.info(f"Successfully executed market order for {symbol} "
                                    f"and removed it from limit orders monitoring list")

        except json.JSONDecodeError:
            logger.error("Failed to parse JSON message")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def listen(self):
        """Listen for incoming messages"""
        try:
            async for message in self.websocket:
                await self.handle_message(message)

                await self.__remove_expired_limit_orders()

                if (updated_symbols := set(await self.db.get_symbols_to_monitor())) != self.symbols:
                    logger.info(f"Symbols to monitor changed: {updated_symbols}. Restarting websocket...")
                    await self.websocket.close()

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error(f"Error in listen loop: {e}")

    async def start_monitoring(self):
        """Start the price monitoring"""
        self.running = True
        logger.info("Starting Bybit crypto price monitor...")

        while self.running:
            try:
                if await self.connect():
                    await self.subscribe_to_tickers()
                    await self.listen()
                else:
                    logger.error("Failed to connect, retrying in 5 seconds...")
                    await asyncio.sleep(5)

            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

            finally:
                if self.websocket:
                    await self.websocket.close()

    def stop(self):
        """Stop the monitoring"""
        self.running = False
        logger.info("Stopping price monitor...")


async def main():
    """Main function to run the price monitor"""
    while True:
        monitor = BybitPriceMonitor(get_settings())

        try:
            await monitor.start_monitoring()
        except KeyboardInterrupt:
            print("\n👋 Monitor stopped. Goodbye!")
            monitor.stop()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
