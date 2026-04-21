# Scalping Bot

A multi-service crypto trading bot that turns Discord trade calls (and Bravos Research alerts) into leveraged perpetual positions on Bybit, with Telegram control, monitoring, and daily stats reporting.

The project is packaged as a single Docker image (`vinnychenko/scalping-bot`) that ships six different worker entrypoints. Which worker runs is selected at startup via the `SERVICE` environment variable.

## Architecture

```
┌───────────────────────┐       ┌───────────────────────┐
│ discord-messages-     │──┐    │ bravos-trade-alerts-  │
│ worker                │  │    │ worker                │
│ (Discord + OpenAI)    │  │    │ (Bravos scraper)      │
└───────────────────────┘  │    └───────────┬───────────┘
                           │                │
                           ▼                ▼
                    ┌──────────────────────────────┐
                    │           Redis              │
                    │  (shared state, queues, TTL) │
                    └──┬───────────────────────┬───┘
                       │                       │
             ┌─────────▼─────────┐   ┌─────────▼─────────┐
             │ prices-worker     │   │ positions-worker  │
             │ (Bybit WS prices) │   │ (Bybit REST poll) │
             └─────────┬─────────┘   └─────────┬─────────┘
                       │                       │
                       └───────────┬───────────┘
                                   ▼
                         ┌───────────────────┐
                         │  Bybit (trades)   │
                         └───────────────────┘

              ┌──────────────────┐   ┌──────────────────┐
              │ telegram-bot     │   │ stats-reporter   │
              │ (commands)       │   │ (daily summary)  │
              └──────────────────┘   └──────────────────┘
```

All workers share configuration via `core/config.py` (a Pydantic `BaseSettings` class loaded from `.env`) and communicate through Redis.

## Services

Each service is started by setting `SERVICE` to one of the values below — see `main.py`:

| `SERVICE` value            | Entrypoint                                                  | What it does |
|----------------------------|-------------------------------------------------------------|--------------|
| `DISCORD_MESSAGES_WORKER`  | `container_services/monitor_discord_messages.py`            | Listens to a configured Discord channel via `discord.py-self`, parses each message with a fine-tuned OpenAI model, and dispatches `CREATE_ORDER` / `CLOSE_ORDER` actions. |
| `PRICES_WORKER`            | `container_services/monitor_prices.py`                      | Subscribes to Bybit public linear WebSocket ticks for all active symbols, expires stale limit orders, and fires market entries once price thresholds are hit. |
| `POSITIONS_WORKER`         | `container_services/monitor_positions.py`                   | Polls Bybit for open positions, manages TP/SL and profit locks, handles fills/retries, and enforces the market-order TTL. |
| `TELEGRAM_BOT`             | `container_services/telegram_bot.py`                        | Operator Telegram bot. Commands: `/help`, `/list_open_positions`, `/list_limit_orders`, `/delete_limit_order`, `/update_ttl`, `/market_depth`, `/params`. |
| `BRAVOS_ALERTS`            | `container_services/bravos_trade_alerts_monitoring.py`      | Scrapes [bravosresearch.com](https://bravosresearch.com/category/portfolio-update/) portfolio updates and forwards new trade alerts to Telegram. |
| `STATS_REPORTER`           | `container_services/stats_reporter.py`                      | Posts a daily P&L / win-rate digest (weekly and monthly rollups) to Telegram at `BOT_STATS_REPORT_HOUR_UTC`. |

## Layout

```
bot/
├── main.py                     # Service dispatcher (switch on SERVICE env var)
├── Dockerfile                  # Python 3.11-slim image, runs main.py
├── docker-compose.yaml         # Redis + all six workers
├── requirements.txt
├── .env                        # Runtime configuration (not committed)
├── container_services/         # One file per worker (long-running entrypoints)
├── core/
│   ├── config.py               # Pydantic Settings (all BOT_*, REDIS_*, API keys)
│   ├── enums.py                # OrderType, EntryType, MessageType
│   ├── logger.py               # Shared logger
│   ├── trading_params.py       # Dynamic TP/SL/leverage resolver
│   └── trading_params.json     # Per-regime, per-balance-tier parameter table
├── services/
│   ├── db_service.py           # Redis-backed state (orders, positions, stats)
│   ├── trading_service.py      # Order-sizing and execution logic
│   └── third_party/
│       ├── bybit_api.py
│       ├── openai_api.py
│       └── telegram_api.py
├── models/                     # OpenAI response schemas
├── utils/                      # Shared helpers (symbol formatting, etc.)
└── testing_tools/
```

## Trading parameters

Take-profit, stop-loss, leverage and risk-per-trade are **not** hard-coded. They are read from `core/trading_params.json` at runtime.

Parameters are indexed by:

1. **Market regime** — `bull` or `bear`, detected from a BTC 1D MA50/MA100 cross (`BOT_MA_CROSS_*` settings).
2. **Balance tier** — the tier whose `[min_balance, max_balance]` range contains the current account balance.

Setting `"disable_bear_trading": true` in the JSON stops new entries while the regime is bear. The values in `core/config.py` are only used as fallbacks when the JSON is missing or malformed.

You can inspect the currently active parameters at any time via the Telegram bot: `/params`.

## Configuration

All configuration is loaded from `bot/.env` by `core/config.Settings`. Key variables:

**Core**
- `LOG_LEVEL`, `LOGS_DIR`, `ENVIRONMENT`

**Risk / sizing** (fallbacks; the JSON wins)
- `BOT_MAX_SIMULTANEOUS_BALANCE_USAGE`, `BOT_MAX_STOP_LOSS_PCT`
- `BOT_RISK_PER_TRADE`, `BOT_LEVERAGE`, `BOT_PRICE_THRESHOLD`
- `BOT_MAX_ORDER_USD`, `BOT_SLIPPAGE_TOLERANCE`, `BOT_MAX_ORDERS_PER_SECOND`
- `BOT_LIMIT_ORDERS_AWAIT_TTL` (days), `BOT_MARKET_ORDERS_TTL` (days)
- `BOT_REQUIRE_STOP_LOSS`, `BOT_BLACKLISTED_COINS`

**Regime detection**
- `BOT_MA_CROSS_SYMBOL`, `BOT_MA_CROSS_SHORT_WINDOW`, `BOT_MA_CROSS_LONG_WINDOW`, `BOT_MA_CROSS_CHECK_INTERVAL_SECONDS`

**Stats reporter**
- `BOT_STATS_REPORT_HOUR_UTC`, `BOT_STATS_RETENTION_DAYS`

**Integrations** (required for the corresponding worker)
- Redis: `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD`, `REDIS_MAX_CONNECTIONS`
- Bybit: `BYBIT_API_KEY`, `BYBIT_API_SECRET`
- Discord: `DISCORD_TOKEN`, `DISCORD_GUILD_ID`, `DISCORD_CHANNEL_ID`
- OpenAI: `OPENAI_API_KEY`, `OPENAI_MODEL_ID`
- Telegram: `TELEGRAM_TRADE_BOT_TOKEN`, `TELEGRAM_SERVICE_BOT_TOKEN`, `TELEGRAM_BRAVOS_STOCKS_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- Bravos: `BRAVOS_COOKIE`

## Running

### Docker Compose (recommended)

Starts Redis and all six workers on a shared `trading-network`, with logs persisted to the `trading_logs` volume:

```bash
cd bot
docker compose up -d
docker compose logs -f positions-worker
```

Each worker pulls the `vinnychenko/scalping-bot` image and has its `SERVICE` env var baked in via compose. To rebuild locally:

```bash
docker build -t vinnychenko/scalping-bot .
docker compose up -d --force-recreate
```

Stop and remove:

```bash
docker compose down           # keep volumes
docker compose down -v        # also wipe redis_data + trading_logs
```

### Running a single worker locally

```bash
cd bot
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export SERVICE=PRICES_WORKER      # or any other service name from the table above
python main.py
```

You need a reachable Redis and a populated `.env` in the `bot/` directory.

## Operating the bot via Telegram

Once `telegram-bot` is running and the `/start` has been sent to `TELEGRAM_TRADE_BOT_TOKEN`:

- `/list_open_positions` — all live Bybit positions managed by the bot
- `/list_limit_orders` — pending limit orders with time-to-expire
- `/delete_limit_order BTC` — cancel a pending limit order for a symbol
- `/update_ttl BTC 4` — shorten a limit order's TTL to N days
- `/market_depth ETH 100 0.5` — inspect order-book depth
- `/params` — print the currently active trading parameters (regime + tier)

## Requirements

- Python 3.11
- Redis 7+ (any recent `redis:alpine` works)
- Bybit USDT-perpetual account with API key
- A fine-tuned OpenAI model that returns the schema in `models/openai_models.py`
- Discord user token (via `discord.py-self`) with access to the target channel
- Telegram bot tokens created via BotFather

See `requirements.txt` for the pinned Python dependencies.

## Room for improvements

- Store tokens and credentials in a cloud secrets manager service (e.g. AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault) instead of a plaintext `.env` file, and load them into `core.config.Settings` at startup.
- Push Docker images to a private cloud artifact registry (e.g. AWS ECR, GCP Artifact Registry, Azure Container Registry) instead of the public Docker Hub repo `vinnychenko/scalping-bot`, and update `docker-compose.yaml` to pull from it with authenticated access.

