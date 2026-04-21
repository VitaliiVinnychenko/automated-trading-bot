import asyncio
import logging
import os
from html import escape
from logging.handlers import RotatingFileHandler

from core.config import get_settings
from services.third_party.telegram_api import TelegramNotifier

# Create logs directory in the app directory (not parent)
logs_dir = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_dir, exist_ok=True)


# Enhanced logging handler that also sends to Telegram
class TelegramLoggingHandler(logging.Handler):
    def __init__(self, notifier: TelegramNotifier):
        super().__init__()
        self.notifier = notifier

    def emit(self, record):
        try:
            message = self.format(record)

            # Get or create event loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            if "leverage not modified" in message:
                return

            # Schedule the coroutine
            message = (f"🚨 <b>ERROR</b>\nModule: {record.module}.{record.filename}\n"
                       f"Function: {record.funcName}\n\n<code>{escape(message)}</code>")

            if loop.is_running():
                asyncio.create_task(self.notifier.send_service_notification(message))
            else:
                loop.run_until_complete(self.notifier.send_service_notification(message))

        except Exception:
            pass  # Avoid infinite recursion if logging fails


config = get_settings()

# Configure logging with both file and console handlers
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Console handler
        logging.StreamHandler(),

        # File handler with rotation
        RotatingFileHandler(
            f"{config.LOGS_DIR}/{os.environ.get('SERVICE', 'service')}.log",
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8"
        )
    ]
)
logger = logging.getLogger(__name__)

# Initialize Telegram notifier
telegram_notifier = TelegramNotifier(logger=logger)

# Add Telegram handler to logger
telegram_handler = TelegramLoggingHandler(telegram_notifier)
telegram_handler.setLevel(logging.ERROR)
logger.addHandler(telegram_handler)
