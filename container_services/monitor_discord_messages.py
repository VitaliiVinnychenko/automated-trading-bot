import asyncio

import discord

from core.config import get_settings
from core.enums import MessageType
from core.logger import logger
from services.db_service import DbService
from services.third_party.openai_api import parse_discord_message_using_openai
from services.third_party.telegram_api import TelegramNotifier
from services.trading_service import handle_close_order_action, handle_new_trade_action
from utils.utils import format_crypto_symbol

config = get_settings()
telegram_notifier = TelegramNotifier(logger=logger)

db = DbService(config)


class TradingBot(discord.Client):

    async def on_ready(self):
        logger.info(f"Bot logged in as {self.user}")

        # Verify we can access the specified guild and channel
        guild = self.get_guild(config.DISCORD_GUILD_ID)
        if guild is None:
            logger.error(f"Could not find guild with ID {config.DISCORD_GUILD_ID}")
            return

        channel = guild.get_channel(config.DISCORD_CHANNEL_ID)
        if channel is None:
            logger.error(f"Could not find channel with ID {config.DISCORD_CHANNEL_ID}")
            return

        logger.info(f"Connected to guild: {guild.name}")
        logger.info(f"Monitoring channel: {channel.name}")

    async def on_message(self, message):
        # Ignore messages from the historical_trades_fetcher itself
        if message.author == self.user:
            return

        # Only process messages from the specified channel
        if message.channel.id != config.DISCORD_CHANNEL_ID:
            return

        # Skip empty messages
        if not message.content.strip():
            return

        logger.info(f"Processing message from {message.author}: {message.content[:100]}...")

        try:
            is_handled = await db.is_discord_message_handled(str(message.id))
            if is_handled:
                logger.info(f"Message (id={message.id}) was already handled before: {message.content[:100]}")
                return
            else:
                await db.mark_discord_message_handled(str(message.id))

            # Parse the message using OpenAI
            parsed_message = await parse_discord_message_using_openai(message.content)

            if parsed_message is None:
                msg = "Failed to parse message after all retries"
                logger.warning(msg)
                await telegram_notifier.send_service_notification(f"{msg}\n<code>{message.content}</code>")
                return

            # Format symbol if it exists
            symbol = format_crypto_symbol(parsed_message.symbol) if parsed_message.symbol else ""

            # Handle different message types
            match parsed_message.message_type:
                case MessageType.CREATE_ORDER:
                    await handle_new_trade_action(symbol, parsed_message)

                case MessageType.TAKE_PROFIT:
                    await handle_close_order_action(symbol, parsed_message)

                case MessageType.CLOSE_ORDER:
                    await handle_close_order_action(symbol, parsed_message)

                case MessageType.ORDER_UPDATE:
                    msg = f"Order update received for {symbol}"
                    logger.info(msg)
                    await telegram_notifier.send_service_notification(msg)

                case MessageType.MARKET_UPDATE:
                    msg = f"Market update received for {symbol}"
                    logger.info(msg)
                    await telegram_notifier.send_service_notification(msg)

                case MessageType.SIMPLE_MESSAGE:
                    msg = "Simple message received - no action needed"
                    logger.info(msg)
                    await telegram_notifier.send_service_notification(msg)

                case _:
                    msg = f"Unknown message type: {parsed_message.message_type}"
                    logger.info(msg)
                    await telegram_notifier.send_service_notification(msg)

        except Exception as e:
            logger.error(f"Error processing message: {e}")


async def main():
    bot = TradingBot()

    try:
        await bot.start(config.DISCORD_TOKEN)
    except discord.LoginFailure:
        logger.error("Invalid Discord token")
    except Exception as e:
        logger.error(f"Bot encountered an error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
