from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Application
    LOG_LEVEL: str = "INFO"
    LOGS_DIR: str = "logs"
    ENVIRONMENT: str = "local"

    # Bot
    BOT_MAX_SIMULTANEOUS_BALANCE_USAGE: float = 0.525  # 52.5%
    BOT_MAX_STOP_LOSS_PCT: float = 0.13
    BOT_LIMIT_ORDERS_AWAIT_TTL: int = 17  # days
    BOT_MARKET_ORDERS_TTL: int = 10  # days
    BOT_RISK_PER_TRADE: float = 0.25  # 25% of balance per trade
    BOT_LEVERAGE: float = 15  # 15x leverage
    BOT_PRICE_THRESHOLD: float = 0.031  # 3.1% threshold for profit locks
    BOT_MAX_ORDER_USD: float = 200000  # 200,000 USDT
    BOT_SLIPPAGE_TOLERANCE: float = 1  # 1% price drop
    BOT_MAX_ORDERS_PER_SECOND: int = 10
    BOT_POSITION_REFRESH_ATTEMPTS: int = 10
    BOT_POSITION_REFRESH_ATTEMPTS_DELAY: int = 3
    BOT_FILL_POSITION_RETRIES: int = 5
    BOT_EXPIRY_CHECK_INTERVAL_SECONDS: int = 60
    BOT_DUMMY_MONITORING_SYMBOL: str = "BTC"

    # BTC MA cross regime detection
    BOT_MA_CROSS_CHECK_INTERVAL_SECONDS: int = 12 * 60 * 60  # 12h
    BOT_MA_CROSS_SYMBOL: str = "BTCUSDT"
    BOT_MA_CROSS_SHORT_WINDOW: int = 50
    BOT_MA_CROSS_LONG_WINDOW: int = 100

    # Dynamic trading params (JSON). Path is resolved relative to the bot
    # package root when not absolute. Values in this file drive TP / SL /
    # leverage / risk-per-trade; the Settings knobs below are fallbacks used
    # only when the JSON is missing or malformed.
    BOT_TRADING_PARAMS_FILE: str = "core/trading_params.json"

    BOT_REQUIRE_STOP_LOSS: bool = True
    BOT_BLACKLISTED_COINS: list[str] = ("ORDER", "BTC", "ETH",)

    # Stats Reporter
    BOT_STATS_REPORT_HOUR_UTC: int = 9
    BOT_STATS_RETENTION_DAYS: int = 90

    # API
    API_MAX_RETRIES: int = 3
    API_BASE_DELAY: float = 1.0
    API_MAX_DELAY: float = 60.0

    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    REDIS_MAX_CONNECTIONS: int = 20

    # Discord
    DISCORD_TOKEN: str | None = None
    DISCORD_GUILD_ID: int | None = None
    DISCORD_CHANNEL_ID: int | None = None

    # OpenAI
    OPENAI_API_KEY: str | None = None
    OPENAI_MODEL_ID: str | None = None

    # Telegram configuration
    TELEGRAM_TRADE_BOT_TOKEN: str | None = None
    TELEGRAM_SERVICE_BOT_TOKEN: str | None = None
    TELEGRAM_BRAVOS_STOCKS_BOT_TOKEN: str | None = None
    TELEGRAM_CHAT_ID: str | None = None

    # Bybit
    BYBIT_API_KEY: str | None = None
    BYBIT_API_SECRET: str | None = None

    # Bravos Research
    BRAVOS_COOKIE: str | None = None

    class Config:
        """
        Tell BaseSettings the env file path
        """

        env_file = "../.env"


@lru_cache()
def get_settings(**kwargs: dict) -> Settings:
    return Settings(**kwargs)
