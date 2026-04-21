import asyncio
import json
import logging
import time

from telegram import Update, BotCommand, BotCommandScopeDefault, BotCommandScopeAllPrivateChats, \
    BotCommandScopeAllGroupChats
from telegram.ext import Application, CommandHandler, ContextTypes

from core.config import Settings, get_settings
from core.logger import logger
from core.trading_params import (
    DEFAULT_REGIME,
    get_loader,
    is_bear_trading_disabled,
    resolve_params,
)
from services.db_service import DbService
from services.third_party.bybit_api import BybitAPI
from services.trading_service import calculate_locked_usdt

logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.ERROR)


class TelegramBotService:
    def __init__(self, config: Settings):
        """Initialize the Telegram bot service."""
        self.config = config
        self.db_service = DbService(config)
        self.bybit_api = BybitAPI(config)

        if not config.TELEGRAM_TRADE_BOT_TOKEN:
            raise ValueError("TELEGRAM_SERVICE_BOT_TOKEN is required")

        self.application = Application.builder().token(config.TELEGRAM_TRADE_BOT_TOKEN).build()

        # Add existing command handlers
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("list_open_positions", self.list_open_positions_command))
        self.application.add_handler(CommandHandler("list_limit_orders", self.list_limit_orders_command))
        self.application.add_handler(CommandHandler("delete_limit_order", self.delete_limit_order_command))
        self.application.add_handler(CommandHandler("update_ttl", self.update_ttl_command))
        self.application.add_handler(CommandHandler("market_depth", self.market_depth_command))
        self.application.add_handler(CommandHandler("params", self.params_command))

        logger.info("Telegram bot service initialized")

    def _build_commands(self) -> list[BotCommand]:
        return [
            BotCommand("help", "Show help"),
            BotCommand("list_open_positions", "List open positions"),
            BotCommand("list_limit_orders", "List limit orders"),
            BotCommand("delete_limit_order", "Delete a limit order: /delete_limit_order BTC"),
            BotCommand("update_ttl", "Update TTL: /update_ttl BTC 4"),
            BotCommand("market_depth", "Get market depth: /market_depth ETH 100 0.5"),
            BotCommand("params", "Show the currently active trading params"),
        ]

    async def _register_commands(self) -> None:
        cmds = self._build_commands()

        # Default (applies everywhere unless overridden)
        await self.application.bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

        # Optional: different sets per scope
        await self.application.bot.set_my_commands(cmds, scope=BotCommandScopeAllPrivateChats())
        await self.application.bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send help information."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        help_message = (
            "🔧 *Trading Bot Commands Help*\n\n"
            "*Regular Commands:*\n\n"
            "📋 `/list_limit_orders`\n"
            "   Shows all active limit orders with current prices and expiration times\n\n"
            "❌ `/delete_limit_order <symbol>`\n"
            "   Deletes a limit order by symbol\n"
            "   Example: `/delete_limit_order BTC`\n\n"
            "⏰ `/update_ttl <symbol> <days>`\n"
            "   Increases or decreases active order TTL by specified days\n"
            "   Example: `/update_ttl BTC 5` (add 5 days)\n\n"
            "📊 `/market_depth <symbol> [leverage] [slippage]`\n"
            "   Analyze market depth with leverage and slippage\n"
            "   Example: `/market_depth ETH 100 0.5` (100x leverage, 0.5% slippage)\n"
            "   Defaults: leverage=1, slippage=1%\n\n"
            "🎚️ `/params`\n"
            "   Show the currently active trading params (regime, balance tier, "
            "TP, SL, leverage, risk-per-trade)"
        )
        await update.message.reply_text(help_message, parse_mode='Markdown')

    async def market_depth_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Get market depth for a symbol with specified leverage and slippage tolerance."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Please provide a symbol.\n"
                "Usage: `/market_depth <symbol> [leverage] [slippage]`\n\n"
                "Examples:\n"
                "• `/market_depth ETH` (default: 1x leverage, 1% slippage)\n"
                "• `/market_depth BTC 50` (50x leverage, 1% slippage)\n"
                "• `/market_depth SOL 100 0.5` (100x leverage, 0.5% slippage)",
                parse_mode='Markdown'
            )
            return

        # Parse arguments
        symbol = context.args[0].upper()
        leverage = 1  # default
        slippage = 1  # default

        try:
            if len(context.args) >= 2:
                leverage = float(context.args[1])
                if leverage <= 0 or leverage > 200:
                    await update.message.reply_text("❌ Leverage must be between 0 and 200")
                    return

            if len(context.args) >= 3:
                slippage = float(context.args[2])
                if slippage < 0 or slippage > 2:
                    await update.message.reply_text("❌ Slippage must be between 0 and 2%")
                    return

        except ValueError:
            await update.message.reply_text("❌ Invalid number format for leverage or slippage")
            return

        full_symbol = f"{symbol}USDT"

        # Send processing message
        processing_msg = await update.message.reply_text(
            f"🔍 Analyzing market depth for *{symbol}*...\n"
            f"• Leverage: {leverage}x\n"
            f"• Slippage tolerance: {slippage}%",
            parse_mode='Markdown'
        )

        try:
            # Get market depth
            market_depth = await self.bybit_api.calculate_market_depth(full_symbol, slippage)

            # Get current price for context
            price_data = await self.bybit_api.get_current_price(full_symbol)
            current_price = price_data.get("mark_price", price_data.get("last_price"))
            trading_params = await self.bybit_api.get_trading_params()

            if not current_price:
                await processing_msg.edit_text(f"❌ Could not retrieve current price for *{symbol}*",
                                               parse_mode='Markdown')
                return

            # Calculate position sizes with leverage
            max_position_usdt = market_depth["ask_depth_usdt"]
            max_position_size = market_depth["ask_quantity"]

            # With leverage, you can open larger positions with less capital
            required_margin = max_position_usdt / leverage

            # Format the response
            depth_message = (
                f"📊 *Market Depth Analysis - {symbol}*\n\n"
                f"💰 *Current Price:* {current_price:,.4f} USDT\n"
                f"⚖️ *Parameters:*\n"
                f"• Leverage: {leverage}x\n"
                f"• Max leverage: {trading_params[symbol]['leverage']}x\n"
                f"• Slippage tolerance: {slippage}%\n\n"
                f"📈 *Market Depth (Buy Side):*\n"
                f"• Max quantity: {max_position_size:,.6f} {symbol}\n"
                f"• Max USDT depth: {max_position_usdt:,.2f} USDT\n\n"
                f"🎯 *Position Calculations:*\n"
                f"• Required margin: {required_margin:,.2f} USDT\n"
                f"• Position value: {max_position_usdt:,.2f} USDT\n\n"
            )

            await processing_msg.edit_text(depth_message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error getting market depth for {symbol}: {str(e)}")
            if 'processing_msg' in locals():
                await processing_msg.edit_text(
                    f"❌ An error occurred while analyzing market depth for *{symbol}*.\n"
                    f"Please check if the symbol exists and try again.",
                    parse_mode='Markdown'
                )
            else:
                await update.message.reply_text(
                    f"❌ An error occurred while analyzing market depth for *{symbol}*.\n"
                    f"Please check if the symbol exists and try again.",
                    parse_mode='Markdown'
                )

    async def params_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the currently active trading params (regime + tier)."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        try:
            regime_raw = await self.db_service.get_market_regime()
            regime = regime_raw or DEFAULT_REGIME
            regime_source = "redis" if regime_raw else "default"

            balance_info = await self.bybit_api.get_usdt_balance()
            balance = balance_info["total_usdt"]

            tier = resolve_params(regime, balance)
            bear_disabled = is_bear_trading_disabled()
            loader_path = get_loader().path

            trading_blocked = regime == "bear" and bear_disabled
            bear_line = (
                "🛑 *Bear trading:* DISABLED (new entries blocked)"
                if bear_disabled
                else "✅ *Bear trading:* enabled"
            )
            blocked_line = (
                "\n⚠️ *New entries are currently blocked (BEAR + disable\\_bear\\_trading).*"
                if trading_blocked
                else ""
            )

            message = (
                f"📊 *Active Trading Params*\n\n"
                f"🌐 *Regime:* {regime.upper()} ({regime_source})\n"
                f"👛 *Balance:* {balance:,.2f} USDT\n"
                f"🗂️ *Source:* {tier.get('source', 'unknown')}\n\n"
                f"🎯 *Take profit:* {tier['take_profit_pct'] * 100:.2f}%\n"
                f"🛡️ *Stop loss (max):* {tier['stop_loss_pct'] * 100:.2f}%\n"
                f"⚡ *Leverage:* {tier['leverage']:.0f}x\n"
                f"💸 *Risk per trade:* {tier['risk_per_trade'] * 100:.2f}%\n\n"
                f"{bear_line}"
                f"{blocked_line}\n\n"
                f"_Config file:_ `{loader_path}`"
            )

            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error fetching trading params: {str(e)}")
            await update.message.reply_text(
                "❌ An error occurred while fetching the current trading params. "
                "Please try again later."
            )

    async def list_limit_orders_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """List all active limit orders."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        try:
            limit_orders = await self.db_service.get_symbols_to_monitor()

            if not limit_orders:
                await update.message.reply_text("📭 No active limit orders found.")
                return

            message_lines = ["📊 *Active limit orders:*\n"]

            for symbol in limit_orders:
                if symbol == self.config.BOT_DUMMY_MONITORING_SYMBOL:
                    continue

                order = await self.db_service.get_limit_order_by_symbol(symbol)

                current_price = await self.bybit_api.get_current_price(f"{symbol}USDT")
                current_price = current_price.get("mark_price", current_price.get("last_price", "N/A"))
                entry_price = order.get('entry_price')

                # Get TTL for this order
                ttl_info = await self._get_order_ttl_info(symbol)

                message_lines.append(
                    f"  • *{symbol}* - {order['order_type']}: {current_price:.4f} USDT (entry: {entry_price:.4f} USDT)")
                message_lines.append(f"     {ttl_info}")
                message_lines.append("")

            message = "\n".join(message_lines)
            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error listing limit orders: {str(e)}")
            await update.message.reply_text(
                "❌ An error occurred while retrieving limit orders. Please try again later."
            )

    async def list_open_positions_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """List all open positions."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        try:
            active_orders = await self.db_service.get_active_orders()

            if not active_orders:
                await update.message.reply_text("📭 No active positions found.")
                return

            message_lines = ["*Open positions:*\n"]

            for symbol in active_orders:
                if symbol == self.config.BOT_DUMMY_MONITORING_SYMBOL:
                    continue

                order = await self.db_service.get_active_order_by_symbol(symbol)

                # Get TTL for this order
                ttl = order["created_at"] + self.config.BOT_MARKET_ORDERS_TTL * 24 * 60 * 60 - time.time()
                ttl_info = self._format_ttl(ttl)

                message_lines.append(f"  • *{symbol} - {order['order_type']}*")
                message_lines.append(f"     Expires in {ttl_info}")
                message_lines.append("")

            balance = (await self.bybit_api.get_usdt_balance())["total_usdt"]
            trading_params = await self.bybit_api.get_trading_params()
            locked_usdt = await calculate_locked_usdt(active_orders, trading_params)

            message_lines.append(
                f"*Available balance used by positions:*\n\n"
                f"  • {locked_usdt / balance * 100:.2f} / {self.config.BOT_MAX_SIMULTANEOUS_BALANCE_USAGE * 100}%\n"
                f"  • {locked_usdt:,.2f} / {balance:,.2f} USDT"
            )
            message = "\n".join(message_lines)
            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error listing active positions: {str(e)}")
            await update.message.reply_text(
                "❌ An error occurred while retrieving active positions. Please try again later."
            )
            raise e

    async def delete_limit_order_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Delete a limit order by symbol."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        if not context.args:
            await update.message.reply_text(
                "❌ Please provide a symbol.\n"
                "Usage: `/delete_limit_order <symbol>`\n"
                "Example: `/delete_limit_order BTC`",
                parse_mode='Markdown'
            )
            return

        symbol = context.args[0].upper()

        try:
            # Check if limit order exists
            if not await self.db_service.has_limit_order(symbol):
                await update.message.reply_text(f"❌ No limit order found for symbol *{symbol}*", parse_mode='Markdown')
                return

            # Delete the limit order
            await self.db_service.remove_symbol_from_monitor(symbol)
            success = await self.db_service.remove_limit_order(symbol)

            if success:
                await update.message.reply_text(f"✅ Limit order for *{symbol}* has been deleted successfully.",
                                                parse_mode='Markdown')
            else:
                await update.message.reply_text(f"❌ Failed to delete limit order for *{symbol}*. Please try again.",
                                                parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error deleting limit order for {symbol}: {str(e)}")
            await update.message.reply_text(
                f"❌ An error occurred while deleting the limit order for *{symbol}*. Please try again later.",
                parse_mode='Markdown'
            )

    async def update_ttl_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Increase or decrease active order TTL by specified days."""
        if update.effective_user.id != int(self.config.TELEGRAM_CHAT_ID):
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ Please provide both symbol and days.\n"
                "Usage: `/update_ttl <symbol> <days>`\n"
                "Example: `/update_ttl BTC 5` (add 5 days)\n"
                "Example: `/update_ttl ETH -2` (subtract 2 days)",
                parse_mode='Markdown'
            )
            return

        symbol = context.args[0].upper()

        try:
            days = int(context.args[1])
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid number of days. Please provide a valid integer.\n"
                "Example: `/update_ttl BTC 5`",
                parse_mode='Markdown'
            )
            return

        try:
            if symbol not in await self.db_service.get_symbols_to_monitor():
                await update.message.reply_text(f"❌ No limit order found for symbol *{symbol}*", parse_mode='Markdown')
                return

            # Get limit order data
            order_data = await self.db_service.get_limit_order_by_symbol(symbol)
            if not order_data:
                await update.message.reply_text(f"❌ Could not retrieve order data for *{symbol}*",
                                                parse_mode='Markdown')
                return

            # Get current TTL
            key = f"{self.db_service.LIMIT_ORDER_PREFIX}{symbol}"
            current_ttl = await self.db_service.redis_client.ttl(key)

            if current_ttl == -2:
                await update.message.reply_text(f"❌ Limit order for *{symbol}* has expired", parse_mode='Markdown')
                return
            elif current_ttl == -1:
                # Key exists but has no TTL, set it to the default + adjustment
                new_ttl = (self.config.BOT_MARKET_ORDERS_TTL * 24 * 60 * 60) + (days * 24 * 60 * 60)
            else:
                # Add/subtract days from current TTL
                new_ttl = current_ttl + (days * 24 * 60 * 60)

            # Ensure TTL is not negative
            if new_ttl <= 0:
                await update.message.reply_text(
                    f"❌ Cannot set TTL to {new_ttl} seconds (non-positive). "
                    f"Current TTL: {current_ttl} seconds",
                    parse_mode='Markdown'
                )
                return

            # Update the order with new TTL
            await self.db_service.redis_client.set(
                key,
                json.dumps(order_data),
                ex=int(new_ttl)
            )

            # Format response message
            action = "increased" if days > 0 else "decreased"
            abs_days = abs(days)
            ttl_info = self._format_ttl(int(new_ttl))

            await update.message.reply_text(
                f"✅ TTL for active order *{symbol}* has been {action} by {abs_days} day(s).\n"
                f"New expiration: {ttl_info}",
                parse_mode='Markdown'
            )

        except Exception as e:
            logger.error(f"Error updating TTL for {symbol}: {str(e)}")
            await update.message.reply_text(
                f"❌ An error occurred while updating TTL for *{symbol}*. Please try again later.",
                parse_mode='Markdown'
            )

    async def _get_order_ttl_info(self, symbol: str) -> str:
        """Get TTL information for a limit order."""
        try:
            key = f"{self.db_service.LIMIT_ORDER_PREFIX}{symbol}"
            ttl_seconds = await self.db_service.redis_client.ttl(key)

            if ttl_seconds == -2:
                return "⚠️ Order has expired"
            elif ttl_seconds == -1:
                return "♾️ No expiration set"
            else:
                return f"Expires in {self._format_ttl(ttl_seconds)}"

        except Exception as e:
            logger.error(f"Error getting TTL for {symbol}: {str(e)}")
            return "❓ TTL unavailable"

    def _format_ttl(self, seconds: int) -> str:
        """Format TTL seconds into human readable format."""
        if seconds <= 0:
            return "expired"

        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        minutes = (seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days:.0f} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours:.0f} hour{'s' if hours != 1 else ''}")
        if minutes > 0 or not parts:  # Show minutes if it's the only unit or if there are no other units
            parts.append(f"{minutes:.0f} minute{'s' if minutes != 1 else ''}")

        return ", ".join(parts)

    async def run(self):
        """Start the Telegram bot."""
        try:
            logger.info("Starting Telegram bot...")
            # Initialize the application
            await self.application.initialize()
            await self.application.start()

            await self._register_commands()

            # Start polling and wait for it to complete
            await self.application.updater.start_polling(allowed_updates=Update.ALL_TYPES)

            # Keep the bot running until interrupted
            try:
                # This will run indefinitely until interrupted
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                logger.info("Bot polling cancelled")

        except Exception as e:
            logger.error(f"Error running Telegram bot: {str(e)}")
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Stop the Telegram bot and cleanup resources."""
        try:
            logger.info("Stopping Telegram bot...")
            if hasattr(self.application.updater, 'stop'):
                await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            await self.db_service.close()
        except Exception as e:
            logger.error(f"Error stopping Telegram bot: {str(e)}")


async def start_telegram_bot():
    """Main function to run the Telegram bot."""
    bot_service = None
    try:
        config = get_settings()
        bot_service = TelegramBotService(config)
        await bot_service.run()
    except KeyboardInterrupt:
        logger.info("Telegram bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in Telegram bot: {str(e)}")
        raise
    finally:
        if bot_service:
            await bot_service.stop()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

    try:
        asyncio.run(start_telegram_bot())
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
