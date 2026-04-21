import asyncio
import json
from datetime import datetime
from typing import Any, List, Dict, Optional

import redis.asyncio as redis

from core.config import Settings, get_settings
from core.logger import logger


class DbService:
    def __init__(self, config: Settings):
        """Initialize the crypto service with Redis connection."""
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True
        )

        # Redis keys
        self.SYMBOLS_KEY = "crypto:symbols_to_monitor"
        self.ACTIVE_ORDERS_KEY = "crypto:active_orders"
        self.DISCORD_MESSAGE_PREFIX = "discord_message:"
        self.ACTIVE_ORDER_PREFIX = "active_order:"
        self.LIMIT_ORDER_PREFIX = "limit_order:"
        self.BRAVOS_LATEST_ARTICLE = "bravos_research:latest_article_url"

        # Dynamic config keys
        self.DYNAMIC_CONFIG_PREFIX = "config:"

        # Stats event keys
        self.STATS_EVENT_WEEKLY_PREFIX = "stats_events:weekly:"
        self.STATS_EVENT_MONTHLY_PREFIX = "stats_events:monthly:"
        self.STATS_SENT_PREFIX = "stats_sent:"

        # Redis keys TTL
        self.LIMIT_ORDER_TTL = config.BOT_LIMIT_ORDERS_AWAIT_TTL * 24 * 60 * 60
        self.MESSAGE_TTL = 60 * 60  # 60 minutes in seconds
        self.STATS_RETENTION_TTL = config.BOT_STATS_RETENTION_DAYS * 24 * 60 * 60

        # Idempotency claim for "position opened" notifications
        self.POSITION_OPENED_CLAIM_PREFIX = "position_opened_claim:"
        self.POSITION_OPENED_CLAIM_TTL = config.BOT_MARKET_ORDERS_TTL * 24 * 60 * 60 * 2

        logger.info("Async DB service initialized")

    async def close(self):
        """Close the Redis connection."""
        await self.redis_client.close()

    # ===== Symbols Monitoring Methods =====

    async def get_symbols_to_monitor(self) -> list[str]:
        """Get list of symbols to monitor."""
        try:
            symbols = await self.redis_client.smembers(self.SYMBOLS_KEY)
            return list(symbols)
        except Exception as e:
            logger.error(f"Error getting symbols to monitor: {str(e)}")
            return []

    async def add_symbol_to_monitor(self, symbol: str) -> bool:
        """Add symbol to list of symbols to monitor."""
        try:
            symbol = symbol.upper()
            result = await self.redis_client.sadd(self.SYMBOLS_KEY, symbol)

            if result:
                logger.info(f"Added symbol {symbol} to monitoring list")
                return True
            else:
                logger.info(f"Symbol {symbol} already in monitoring list")
                return True

        except Exception as e:
            logger.error(f"Error adding symbol {symbol} to monitor: {str(e)}")
            return False

    async def remove_symbol_from_monitor(self, symbol: str) -> bool:
        """Remove symbol from list of symbols to monitor."""
        try:
            symbol = symbol.upper()
            result = await self.redis_client.srem(self.SYMBOLS_KEY, symbol)

            if result:
                logger.info(f"Removed symbol {symbol} from monitoring list")
                return True
            else:
                logger.warning(f"Symbol {symbol} not found in monitoring list")
                return False

        except Exception as e:
            logger.error(f"Error removing symbol {symbol} from monitor: {str(e)}")
            return False

    async def remove_symbols_from_monitor(self, symbols: list[str]) -> int:
        """Remove multiple symbols from the monitoring set in a single round-trip."""
        if not symbols:
            return 0
        try:
            normalized = [s.upper() for s in symbols]
            removed = await self.redis_client.srem(self.SYMBOLS_KEY, *normalized)
            if removed:
                logger.info(f"Removed {removed} symbol(s) from monitoring list: {normalized}")
            return int(removed or 0)
        except Exception as e:
            logger.error(f"Error removing symbols {symbols} from monitor: {str(e)}")
            return 0

    # ===== Active Orders Methods =====

    async def get_active_orders(self) -> list[str]:
        """Get list of active order symbols."""
        try:
            orders = await self.redis_client.smembers(self.ACTIVE_ORDERS_KEY)
            return list(orders)
        except Exception as e:
            logger.error(f"Error getting active orders: {str(e)}")
            return []

    async def add_active_order(self, symbol: str) -> bool:
        """Add symbol to active orders set."""
        try:
            symbol = symbol.upper()
            result = await self.redis_client.sadd(self.ACTIVE_ORDERS_KEY, symbol)

            if result:
                logger.info(f"Added symbol {symbol} to active orders")
                return True
            else:
                logger.info(f"Symbol {symbol} already in active orders")
                return True

        except Exception as e:
            logger.error(f"Error adding symbol {symbol} to active orders: {str(e)}")
            return False

    async def remove_active_order(self, symbol: str) -> bool:
        """Remove symbol from active orders set."""
        try:
            symbol = symbol.upper()
            result = await self.redis_client.srem(self.ACTIVE_ORDERS_KEY, symbol)

            if result:
                logger.info(f"Removed symbol {symbol} from active orders")
                return True
            else:
                logger.warning(f"Symbol {symbol} not found in active orders")
                return False

        except Exception as e:
            logger.error(f"Error removing symbol {symbol} from active orders: {str(e)}")
            return False

    # ===== Discord Message Handling Methods =====

    async def mark_discord_message_handled(self, message_id: str) -> bool:
        """Mark a Discord message as handled with a 60-minute TTL."""
        try:
            key = f"{self.DISCORD_MESSAGE_PREFIX}{message_id}"
            await self.redis_client.set(key, 1, ex=self.MESSAGE_TTL)
            logger.info(f"Marked Discord message {message_id} as handled (TTL: 60 minutes)")
            return True
        except Exception as e:
            logger.error(f"Error marking Discord message {message_id} as handled: {str(e)}")
            return False

    async def is_discord_message_handled(self, message_id: str) -> bool:
        """Check if a Discord message has been handled."""
        try:
            key = f"{self.DISCORD_MESSAGE_PREFIX}{message_id}"
            result = await self.redis_client.exists(key)
            return bool(result)
        except Exception as e:
            logger.error(f"Error checking if Discord message {message_id} was handled: {str(e)}")
            return False

    # ===== Limit Order Methods =====

    async def create_limit_order(self, symbol: str, order_data: dict[str, Any]) -> bool:
        """
        Create a limit order record for a symbol with JSON data and the configured TTL.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")
            order_data (Dict[str, Any]): Order data to store as JSON

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            symbol = symbol.upper()
            key = f"{self.LIMIT_ORDER_PREFIX}{symbol}"

            # Convert to JSON string
            json_data = json.dumps(order_data)

            # Set the key with JSON data and the configured TTL
            await self.redis_client.set(key, json_data, ex=self.LIMIT_ORDER_TTL)

            logger.info(f"Created limit order for symbol {symbol} (TTL: {self.LIMIT_ORDER_TTL / 86400:.1f} days)")
            return True

        except Exception as e:
            logger.error(f"Error creating limit order for symbol {symbol}: {str(e)}")
            return False

    async def has_limit_order(self, symbol: str) -> bool:
        """
        Check if a limit order exists for the given symbol.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")

        Returns:
            bool: True if a limit order exists, False otherwise
        """
        try:
            symbol = symbol.upper()
            key = f"{self.LIMIT_ORDER_PREFIX}{symbol}"

            # Check if the key exists
            result = await self.redis_client.exists(key)
            return bool(result)

        except Exception as e:
            logger.error(f"Error checking limit order for symbol {symbol}: {str(e)}")
            return False

    async def get_limit_order_by_symbol(self, symbol: str) -> dict[str, Any] | None:
        """
        Get limit order data for the given symbol.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")

        Returns:
            Optional[Dict[str, Any]]: Order data if exists, None otherwise
        """
        try:
            symbol = symbol.upper()
            key = f"{self.LIMIT_ORDER_PREFIX}{symbol}"

            # Get the JSON data
            json_data = await self.redis_client.get(key)

            if json_data:
                return json.loads(json_data)
            else:
                logger.warning(f"No limit order found for symbol {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error getting limit order for symbol {symbol}: {str(e)}")
            return None

    async def get_limit_orders_by_symbols(self, symbols: list[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch limit orders for many symbols in a single pipelined round-trip.

        Missing keys are omitted from the result (same semantics as the singular
        getter returning None, just hoisted to the dict level).
        """
        if not symbols:
            return {}
        try:
            normalized = [s.upper() for s in symbols]
            async with self.redis_client.pipeline(transaction=False) as pipe:
                for s in normalized:
                    pipe.get(f"{self.LIMIT_ORDER_PREFIX}{s}")
                values = await pipe.execute()
            return {
                s: json.loads(v)
                for s, v in zip(normalized, values)
                if v
            }
        except Exception as e:
            logger.error(f"Error getting limit orders for symbols {symbols}: {str(e)}")
            return {}

    async def remove_limit_order(self, symbol: str) -> bool:
        """
        Remove a limit order for the given symbol.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")

        Returns:
            bool: True if successful, False if no order found
        """
        try:
            symbol = symbol.upper()
            key = f"{self.LIMIT_ORDER_PREFIX}{symbol}"

            # Delete the key
            result = await self.redis_client.delete(key)

            if result:
                logger.info(f"Removed limit order for symbol {symbol}")
                return True
            else:
                logger.warning(f"No limit order found for symbol {symbol}")
                return False

        except Exception as e:
            logger.error(f"Error removing limit order for symbol {symbol}: {str(e)}")
            return False

    # ===== Pattern Matching Methods for Limit Orders =====

    async def get_all_limit_order_keys(self) -> List[str]:
        """
        Get all keys matching the limit_order:* pattern.

        Returns:
            List[str]: List of all limit order keys
        """
        try:
            keys = []
            pattern = f"{self.LIMIT_ORDER_PREFIX}*"

            # Use scan_iter to get all matching keys without blocking Redis
            async for key in self.redis_client.scan_iter(match=pattern):
                keys.append(key)

            logger.info(f"Found {len(keys)} limit order keys")
            return keys
        except Exception as e:
            logger.error(f"Error getting limit order keys: {str(e)}")
            return []

    async def get_all_limit_order_symbols(self) -> List[str]:
        """
        Extract symbols from all limit_order:* keys.

        Returns:
            List[str]: List of symbols from limit order keys
        """
        try:
            keys = await self.get_all_limit_order_keys()
            symbols = []

            # Extract symbol from each key (removing the prefix)
            prefix_len = len(self.LIMIT_ORDER_PREFIX)
            for key in keys:
                symbol = key[prefix_len:]
                symbols.append(symbol)

            return symbols
        except Exception as e:
            logger.error(f"Error extracting symbols from limit order keys: {str(e)}")
            return []

    async def get_limit_order_count(self) -> int:
        """
        Count the number of limit_order:* keys.

        Returns:
            int: Count of limit order keys
        """
        try:
            keys = await self.get_all_limit_order_keys()
            return len(keys)
        except Exception as e:
            logger.error(f"Error counting limit order keys: {str(e)}")
            return 0

    # ===== Active Order Methods =====

    async def create_active_order_by_symbol(self, symbol: str, order_data: dict[str, Any]) -> bool:
        """
        Create an active order record for a symbol with JSON data and the configured TTL.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")
            order_data (Dict[str, Any]): Order data to store as JSON

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            symbol = symbol.upper()
            key = f"{self.ACTIVE_ORDER_PREFIX}{symbol}"

            # Convert to JSON string
            json_data = json.dumps(order_data)

            # Set the key with JSON data
            await self.redis_client.set(key, json_data)

            logger.info(f"Created market order for symbol {symbol}")
            return True

        except Exception as e:
            logger.error(f"Error creating market order for symbol {symbol}: {str(e)}")
            return False

    async def update_active_order_by_symbol(self, symbol: str, order_data: dict[str, Any]) -> bool:
        """
        Update an active order record for a symbol with new JSON data while preserving the existing TTL.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")
            order_data (Dict[str, Any]): New order data to store as JSON

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            symbol = symbol.upper()
            key = f"{self.ACTIVE_ORDER_PREFIX}{symbol}"

            # Check if the key exists first
            exists = await self.redis_client.exists(key)
            if not exists:
                logger.warning(f"No active order found for symbol {symbol} to update")
                return False

            # Get the current TTL
            current_ttl = await self.redis_client.ttl(key)

            # Convert to JSON string
            json_data = json.dumps(order_data)

            # Update the key with new data and preserve TTL
            if current_ttl > 0:
                # Key has a TTL, preserve it
                await self.redis_client.set(key, json_data, ex=current_ttl)
                logger.info(f"Updated active order for symbol {symbol} (preserved TTL: {current_ttl} seconds)")
            elif current_ttl == -1:
                # Key exists but has no TTL (shouldn't happen in this use case, but handle it)
                await self.redis_client.set(key, json_data)
                logger.info(f"Updated active order for symbol {symbol} (no TTL)")
            else:
                # TTL is -2, meaning key doesn't exist or expired during our operation
                logger.warning(f"Active order for symbol {symbol} expired during update operation")
                return False

            return True

        except Exception as e:
            logger.error(f"Error updating active order for symbol {symbol}: {str(e)}")
            return False

    async def get_active_order_by_symbol(self, symbol: str) -> dict[str, Any] | None:
        """
        Get active order data for the given symbol.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")

        Returns:
            Optional[Dict[str, Any]]: Order data if exists, None otherwise
        """
        try:
            symbol = symbol.upper()
            key = f"{self.ACTIVE_ORDER_PREFIX}{symbol}"

            # Get the JSON data
            json_data = await self.redis_client.get(key)

            if json_data:
                return json.loads(json_data)
            else:
                logger.warning(f"No market order found for symbol {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error getting market order for symbol {symbol}: {str(e)}")
            return None

    async def get_active_orders_by_symbols(self, symbols: list[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch active orders for many symbols in a single pipelined round-trip.

        Missing keys are omitted from the returned dict.
        """
        if not symbols:
            return {}
        try:
            normalized = [s.upper() for s in symbols]
            async with self.redis_client.pipeline(transaction=False) as pipe:
                for s in normalized:
                    pipe.get(f"{self.ACTIVE_ORDER_PREFIX}{s}")
                values = await pipe.execute()
            return {
                s: json.loads(v)
                for s, v in zip(normalized, values)
                if v
            }
        except Exception as e:
            logger.error(f"Error getting active orders for symbols {symbols}: {str(e)}")
            return {}

    async def remove_active_order_by_symbol(self, symbol: str) -> bool:
        """
        Remove a active order for the given symbol.

        Args:
            symbol (str): Trading symbol (e.g., "BTC")

        Returns:
            bool: True if successful, False if no order found
        """
        try:
            symbol = symbol.upper()
            key = f"{self.ACTIVE_ORDER_PREFIX}{symbol}"

            # Delete the key
            result = await self.redis_client.delete(key)

            if result:
                logger.info(f"Removed market order for symbol {symbol}")
                return True
            else:
                logger.warning(f"No market order found for symbol {symbol}")
                return False

        except Exception as e:
            logger.error(f"Error removing market order for symbol {symbol}: {str(e)}")
            return False

    async def claim_position_opened(self, symbol: str, created_at: float) -> bool:
        """
        Atomically claim the completion of 'position opened' for a specific order.

        Returns True exactly once per (symbol, created_at). Subsequent callers get False,
        which callers should treat as 'already handled, skip side effects'.
        On Redis failure returns True (fail-open) so we prefer a rare duplicate over a
        missed notification.
        """
        try:
            symbol = symbol.upper()
            claim_key = f"{self.POSITION_OPENED_CLAIM_PREFIX}{symbol}:{int(created_at)}"
            result = await self.redis_client.set(
                claim_key, "1", nx=True, ex=self.POSITION_OPENED_CLAIM_TTL,
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Error claiming position opened for {symbol}: {e}")
            return True

    # ===== Pattern Matching Methods for Active Orders =====

    async def get_all_active_order_keys(self) -> List[str]:
        """
        Get all keys matching the active_order:* pattern.

        Returns:
            List[str]: List of all active order keys
        """
        try:
            keys = []
            pattern = f"{self.ACTIVE_ORDER_PREFIX}*"

            # Use scan_iter to get all matching keys without blocking Redis
            async for key in self.redis_client.scan_iter(match=pattern):
                keys.append(key)

            logger.info(f"Found {len(keys)} active order keys")
            return keys
        except Exception as e:
            logger.error(f"Error getting active order keys: {str(e)}")
            return []

    async def get_all_active_order_symbols(self) -> List[str]:
        """
        Extract symbols from all active_order:* keys.

        Returns:
            List[str]: List of symbols from active order keys
        """
        try:
            keys = await self.get_all_active_order_keys()
            symbols = []

            # Extract symbol from each key (removing the prefix)
            prefix_len = len(self.ACTIVE_ORDER_PREFIX)
            for key in keys:
                symbol = key[prefix_len:]
                symbols.append(symbol)

            return symbols
        except Exception as e:
            logger.error(f"Error extracting symbols from active order keys: {str(e)}")
            return []

    async def get_active_order_count(self) -> int:
        """
        Count the number of active_order:* keys.

        Returns:
            int: Count of active order keys
        """
        try:
            keys = await self.get_all_active_order_keys()
            return len(keys)
        except Exception as e:
            logger.error(f"Error counting active order keys: {str(e)}")
            return 0

    async def get_latest_bravos_article_url(self) -> Optional[str]:
        """
        Get the latest processed Bravos Research article URL from Redis.

        Returns:
            Optional[str]: Latest article URL if exists, None otherwise
        """
        try:
            url = await self.redis_client.get(self.BRAVOS_LATEST_ARTICLE)

            if url:
                logger.info(f"Latest Bravos article URL from Redis: {url}")
            else:
                logger.info("No previous Bravos article URL found in Redis")

            return url
        except Exception as e:
            logger.error(f"Error getting latest Bravos article URL: {str(e)}")
            return None

    async def set_latest_bravos_article_url(self, url: str) -> bool:
        """
        Save the latest processed Bravos Research article URL to Redis.

        Args:
            url: Article URL to save

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            await self.redis_client.set(self.BRAVOS_LATEST_ARTICLE, url)
            logger.info(f"Saved latest Bravos article URL to Redis: {url}")
            return True
        except Exception as e:
            logger.error(f"Error setting latest Bravos article URL: {str(e)}")
            return False

    # ===== Dynamic Config Methods =====

    async def get_market_regime(self) -> str | None:
        """
        Get the active BTC market regime ("bull" / "bear") from Redis.

        Returns:
            str | None: Regime value if set, None otherwise.
        """
        try:
            value = await self.redis_client.get(f"{self.DYNAMIC_CONFIG_PREFIX}market_regime")
            return value or None
        except Exception as e:
            logger.error(f"Error getting market regime: {str(e)}")
            return None

    async def set_market_regime(self, regime: str) -> bool:
        """
        Persist the active BTC market regime ("bull" / "bear") in Redis.

        Args:
            regime: Regime label, one of "bull" or "bear".

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            await self.redis_client.set(f"{self.DYNAMIC_CONFIG_PREFIX}market_regime", regime)
            logger.info(f"Set market regime to {regime}")
            return True
        except Exception as e:
            logger.error(f"Error setting market regime: {str(e)}")
            return False

    # ===== Stats Event Methods =====

    async def record_position_event(self, event_data: dict) -> bool:
        """
        Append a position open/close event to both weekly and monthly Redis lists.
        Sets a retention TTL on first element.
        """
        try:
            now = datetime.utcnow()
            week_key = f"{self.STATS_EVENT_WEEKLY_PREFIX}{now.strftime('%G-W%V')}"
            month_key = f"{self.STATS_EVENT_MONTHLY_PREFIX}{now.strftime('%Y-%m')}"

            json_data = json.dumps(event_data)

            for key in (week_key, month_key):
                length = await self.redis_client.rpush(key, json_data)
                if length == 1:
                    await self.redis_client.expire(key, self.STATS_RETENTION_TTL)

            logger.info(
                f"Recorded stats event: {event_data['event_type']} {event_data['symbol']}"
            )
            return True
        except Exception as e:
            logger.error(f"Error recording position event: {str(e)}")
            return False

    async def get_stats_events(self, prefix: str, period_key: str) -> List[Dict[str, Any]]:
        """Retrieve all events from a stats event list."""
        try:
            key = f"{prefix}{period_key}"
            raw_events = await self.redis_client.lrange(key, 0, -1)
            return [json.loads(item) for item in raw_events]
        except Exception as e:
            logger.error(f"Error getting stats events for {prefix}{period_key}: {str(e)}")
            return []

    async def mark_stats_sent(self, report_type: str, period_key: str) -> bool:
        """Mark a report as sent with a 7-day TTL to prevent duplicates."""
        try:
            key = f"{self.STATS_SENT_PREFIX}{report_type}:{period_key}"
            await self.redis_client.set(key, "1", ex=7 * 24 * 60 * 60)
            return True
        except Exception as e:
            logger.error(f"Error marking stats sent for {report_type}:{period_key}: {str(e)}")
            return False

    async def is_stats_sent(self, report_type: str, period_key: str) -> bool:
        """Check if a report has already been sent for the given period."""
        try:
            key = f"{self.STATS_SENT_PREFIX}{report_type}:{period_key}"
            return bool(await self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Error checking stats sent for {report_type}:{period_key}: {str(e)}")
            return False


async def main():
    config = get_settings()
    db = DbService(config)

    await db.create_active_order_by_symbol("MORPHO", {
        "stop_loss": 1.6245,
        "order_type": "LONG",
        "quantity": 2420,
        "average_price": 1.8623,
        "is_filled": True,
    })


if __name__ == "__main__":
    asyncio.run(main())
