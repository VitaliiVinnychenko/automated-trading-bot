import asyncio
import os

from container_services.monitor_discord_messages import main as monitor_discord_messages_worker
from container_services.monitor_positions import main as monitor_positions_worker
from container_services.monitor_prices import main as monitor_prices_worker
from container_services.telegram_bot import start_telegram_bot
from container_services.bravos_trade_alerts_monitoring import main as bravos_trade_alerts_monitoring
from container_services.stats_reporter import main as stats_reporter

if __name__ == "__main__":
    service = os.environ.get("SERVICE")

    match service:
        case "PRICES_WORKER":
            asyncio.run(monitor_prices_worker())
        case "POSITIONS_WORKER":
            asyncio.run(monitor_positions_worker())
        case "DISCORD_MESSAGES_WORKER":
            asyncio.run(monitor_discord_messages_worker())
        case "TELEGRAM_BOT":
            asyncio.run(start_telegram_bot())
        case "BRAVOS_ALERTS":
            asyncio.run(bravos_trade_alerts_monitoring())
        case "STATS_REPORTER":
            asyncio.run(stats_reporter())
        case _:
            print(f"Unknown service: {service}")
