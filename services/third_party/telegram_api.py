import aiohttp

from core.config import get_settings

config = get_settings()


class TelegramNotifier:
    def __init__(self, logger):
        self.session = None
        self.logger = logger

    async def create_session(self):
        """Create aiohttp session for Telegram API calls"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    async def send_message(self, bot_token: str, chat_id: str, message: str, parse_mode: str = "HTML"):
        """Send message to Telegram bot"""
        if not self.session:
            await self.create_session()

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": False
        }

        try:
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    self.logger.debug(f"Message sent successfully to Telegram chat {chat_id}")
                else:
                    response_text = await response.text()
                    self.logger.error(f"Failed to send Telegram message: {response.status} - {response_text}")

        except Exception as e:
            self.logger.error(f"Error sending Telegram message: {e}")

    async def send_trade_update(self, message: str):
        """Send message to trade updates bot"""
        await self.send_message(
            config.TELEGRAM_TRADE_BOT_TOKEN,
            config.TELEGRAM_CHAT_ID,
            message
        )

    async def send_service_notification(self, message: str):
        """Send message to service notifications bot"""
        await self.send_message(
            config.TELEGRAM_SERVICE_BOT_TOKEN,
            config.TELEGRAM_CHAT_ID,
            message
        )

    async def send_article_update(self, message: str):
        """
        Send article content to trade updates bot.

        This is specifically for sending scraped article content with proper HTML formatting.
        The message should already be formatted with HTML tags for title, content, and links.

        Args:
            message: HTML-formatted message containing article content
        """
        await self.send_message(
            config.TELEGRAM_BRAVOS_STOCKS_BOT_TOKEN,
            config.TELEGRAM_CHAT_ID,
            message,
            parse_mode="HTML"
        )