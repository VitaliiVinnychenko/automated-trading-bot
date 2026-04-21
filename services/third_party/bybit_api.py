import asyncio
import hashlib
import hmac
import json
import time
from typing import Optional, Dict, Union

import aiohttp
from aiohttp import ClientTimeout, ClientSession
from async_lru import alru_cache

from core.config import get_settings, Settings
from core.logger import logger


class BybitAPI:
    def __init__(self, config: Settings,
                 timeout: int = 30, max_retries: int = 3, backoff_factor: float = 0.3):
        self.base_url = "https://api.bybit.com"
        self.api_key = config.BYBIT_API_KEY
        self.api_secret = config.BYBIT_API_SECRET
        self.config = config
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        # Configure logging
        self.logger = logger

        # Session will be created when needed
        self._session: Optional[ClientSession] = None

    async def _get_session(self) -> ClientSession:
        """Get or create aiohttp session with retry configuration"""
        if self._session is None or self._session.closed:
            timeout = ClientTimeout(total=self.timeout)

            # Create connector with connection pooling
            connector = aiohttp.TCPConnector(
                limit=20,  # Total connection pool size
                limit_per_host=10,  # Connections per host
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True,
            )

            # Set default headers
            headers = {
                'User-Agent': 'BybitAPI/1.0',
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }

            self._session = ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers
            )

        return self._session

    async def close(self):
        """Close the session and clean up resources"""
        if self._session and not self._session.closed:
            await self._session.close()
            self.logger.info("Session closed successfully")

    def _generate_signature(self, timestamp: str, params: str) -> str:
        """Generate signature for authenticated requests"""
        if not self.api_secret:
            raise ValueError("API secret is required for trading operations")

        param_str = timestamp + self.api_key + "5000" + params
        return hmac.new(
            bytes(self.api_secret, "utf-8"),
            param_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    async def _make_request_with_retry(self, method: str, url: str, **kwargs) -> dict:
        """Make HTTP request with exponential backoff retry"""
        session = await self._get_session()

        for attempt in range(self.max_retries + 1):
            try:
                async with session.request(method, url, **kwargs) as response:
                    # Check for rate limiting
                    if response.status == 429:
                        if attempt < self.max_retries:
                            wait_time = self.backoff_factor * (2 ** attempt)
                            self.logger.warning(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}")
                            await asyncio.sleep(wait_time)
                            continue

                    # Check for server errors that should be retried
                    if response.status in [500, 502, 503, 504]:
                        if attempt < self.max_retries:
                            wait_time = self.backoff_factor * (2 ** attempt)
                            self.logger.warning(f"Server error {response.status}, retrying in {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue

                    # Raise for other HTTP errors
                    response.raise_for_status()

                    # Parse JSON response
                    data = await response.json()
                    return data

            except asyncio.TimeoutError:
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor * (2 ** attempt)
                    self.logger.warning(f"Request timeout, retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Request timeout after {self.max_retries} retries")

            except aiohttp.ClientError as e:
                if attempt < self.max_retries:
                    wait_time = self.backoff_factor * (2 ** attempt)
                    self.logger.warning(f"Client error: {e}, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Request failed after {self.max_retries} retries: {str(e)}")

        raise Exception(f"Request failed after {self.max_retries} retries")

    async def _make_authenticated_request(self, method: str, endpoint: str, params: dict = None) -> dict:
        """Make authenticated request to Bybit API with retry mechanism"""
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret are required for trading operations")

        timestamp = str(int(time.time() * 1000))

        if params is None:
            params = {}

        # Convert params to string for signature based on method
        if method.upper() == "GET":
            # For GET requests, use URL-encoded format
            param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        else:
            # For POST requests, use JSON string format
            param_str = json.dumps(params) if params else ""

        signature = self._generate_signature(timestamp, param_str)

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000",
            "X-BAPI-SIGN-TYPE": "2",
        }

        url = f"{self.base_url}{endpoint}"

        try:
            if method.upper() == "GET":
                data = await self._make_request_with_retry("GET", url, params=params, headers=headers)
            elif method.upper() == "POST":
                data = await self._make_request_with_retry("POST", url, json=params, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if data.get("retCode") != 0:
                error_msg = f"Bybit API error: {data.get('retMsg', 'Unknown error')} (Code: {data.get('retCode')})"
                self.logger.error(error_msg)
                raise Exception(error_msg)

            self.logger.debug(f"Successful {method} request to {endpoint}")
            return data

        except Exception as e:
            if "Request timeout" in str(e) or "Request failed" in str(e):
                self.logger.error(f"{str(e)} for {method} {endpoint}")
                raise
            else:
                error_msg = f"Request failed for {method} {endpoint}: {str(e)}"
                self.logger.error(error_msg)
                raise Exception(error_msg)

    async def _make_public_request(self, endpoint: str, params: dict = None) -> dict:
        """Make public API request with retry mechanism"""
        url = f"{self.base_url}{endpoint}"

        try:
            data = await self._make_request_with_retry("GET", url, params=params)

            if data.get("retCode") != 0:
                error_msg = f"Bybit API error: {data.get('retMsg', 'Unknown error')} (Code: {data.get('retCode')})"
                self.logger.error(error_msg)
                raise Exception(error_msg)

            self.logger.debug(f"Successful public request to {endpoint}")
            return data

        except Exception as e:
            if "Request timeout" in str(e) or "Request failed" in str(e):
                self.logger.error(f"{str(e)} for {endpoint}")
                raise
            else:
                error_msg = f"Public request failed for {endpoint}: {str(e)}"
                self.logger.error(error_msg)
                raise Exception(error_msg)

    @alru_cache(maxsize=1, ttl=3600)
    async def get_trading_params(self) -> Dict[str, Dict[str, float]]:
        """Get instruments info with pagination support"""
        endpoint = "/v5/market/instruments-info"

        # Parameters for linear futures (USDT perpetuals)
        params = {
            "category": "linear"
        }

        next_page_cursor = "111"
        instruments = {}

        while next_page_cursor:
            data = await self._make_public_request(endpoint, params)
            params["cursor"] = next_page_cursor = data["result"]["nextPageCursor"]

            if "list" in data["result"]:
                for item in data["result"]["list"]:
                    # Filter for USDT perpetuals only
                    if item["quoteCoin"] == "USDT" and item["status"] == "Trading" and item[
                        "symbol"] == f"{item['baseCoin']}USDT":
                        # Extract max leverage
                        max_leverage = 0
                        if "leverageFilter" in item and isinstance(item["leverageFilter"], dict):
                            max_leverage = item["leverageFilter"].get("maxLeverage", 0)

                        # Add to result list
                        instruments[item["baseCoin"]] = {
                            "leverage": float(max_leverage),
                            "maxOrderQty": float(item["lotSizeFilter"]["maxMktOrderQty"]),
                            "minOrderQty": float(item["lotSizeFilter"]["minOrderQty"]),
                            "minNotionalValue": float(item["lotSizeFilter"]["minNotionalValue"]),
                            "priceScale": int(item["priceScale"]),
                        }

        return instruments

    async def get_current_price(self, symbol: str, category: str = "linear") -> dict:
        """
        Get current market price and ticker information for a symbol

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            category (str): Product category ('spot', 'linear', 'inverse', 'option'). Default is 'linear'

        Returns:
            dict: Current price data including:
                - symbol: Trading pair symbol
                - last_price: Current/last traded price
                - bid_price: Best bid price
                - ask_price: Best ask price
                - mark_price: Mark price (for derivatives)
                - index_price: Index price (for derivatives)
                - high_24h: 24h high price
                - low_24h: 24h low price
                - volume_24h: 24h trading volume
                - turnover_24h: 24h turnover
                - price_change_24h: 24h price change
                - price_change_24h_pct: 24h price change percentage
                - prev_price_24h: Price 24h ago

        Raises:
            ValueError: If symbol is not found or invalid
            Exception: If API request fails
        """
        endpoint = "/v5/market/tickers"

        params = {
            "category": category,
            "symbol": symbol
        }

        try:
            self.logger.debug(f"Fetching current price for {symbol}")

            data = await self._make_public_request(endpoint, params)

            # Extract ticker data
            result = data.get("result", {})
            tickers = result.get("list", [])

            if not tickers:
                raise ValueError(f"No ticker data found for symbol {symbol}")

            ticker = tickers[0]  # Should only be one ticker for the specific symbol

            # Parse and format the ticker data
            price_data = {
                "symbol": ticker.get("symbol"),
                "last_price": float(ticker.get("lastPrice", 0)),
                "bid_price": float(ticker.get("bid1Price", 0)),
                "ask_price": float(ticker.get("ask1Price", 0)),
                "mark_price": float(ticker.get("markPrice", 0)) if ticker.get("markPrice") else None,
                "index_price": float(ticker.get("indexPrice", 0)) if ticker.get("indexPrice") else None,
                "high_24h": float(ticker.get("highPrice24h", 0)),
                "low_24h": float(ticker.get("lowPrice24h", 0)),
                "volume_24h": float(ticker.get("volume24h", 0)),
                "turnover_24h": float(ticker.get("turnover24h", 0)),
                "price_change_24h": float(ticker.get("price24hPcnt", 0)),
                "price_change_24h_pct": round(float(ticker.get("price24hPcnt", 0)) * 100, 4),
                "prev_price_24h": float(ticker.get("prevPrice24h", 0)),
                "timestamp": int(time.time() * 1000),
                "category": category
            }

            self.logger.info(f"Current price for {symbol}: ${price_data['last_price']:.4f}")

            return price_data

        except Exception as e:
            self.logger.error(f"Error fetching current price for {symbol}: {str(e)}")
            raise

    async def get_kline(
        self,
        symbol: str,
        interval: str = "D",
        limit: int = 200,
        category: str = "linear",
    ) -> list[dict]:
        """Fetch kline/candlestick data from Bybit.

        Returns a newest-first list of dicts with keys:
        start_ms, open, high, low, close, volume, turnover.
        """
        endpoint = "/v5/market/kline"
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }

        data = await self._make_public_request(endpoint, params)
        raw = data.get("result", {}).get("list", [])

        return [
            {
                "start_ms": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "turnover": float(row[6]),
            }
            for row in raw
        ]

    async def get_order_book(self, symbol: str, category: str = "linear", limit: int = 200) -> dict:
        """
        Fetch order book data from Bybit API

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            category (str): Product category ('spot', 'linear', 'inverse', 'option')
            limit (int): Number of order book entries to retrieve (1-200)

        Returns:
            dict: Order book data with bids and asks
        """
        endpoint = "/v5/market/orderbook"

        params = {
            "category": category,
            "symbol": symbol,
            "limit": limit
        }

        data = await self._make_public_request(endpoint, params)
        return data["result"]

    async def calculate_market_depth(self, symbol: str, max_price_change_pct: float = 0.5,
                                     category: str = "linear", limit: int = 200) -> dict:
        """
        Calculate market depth in USDT and determine max allowed price change for trades.

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            max_price_change_pct (float): Maximum allowed price change percentage (default 0.5%)
            category (str): Product category ('spot', 'linear', 'inverse', 'option')
            limit (int): Number of order book entries to retrieve

        Returns:
            dict: Market depth analysis including:
                - symbol: Trading pair symbol
                - bid_depth_usdt: Total USDT value of bids within price range
                - ask_depth_usdt: Total USDT value of asks within price range
        """

        # Get order book data from Bybit API
        order_book_data = await self.get_order_book(symbol, category, limit)

        bids = order_book_data.get('b', [])  # Bybit uses 'b' for bids
        asks = order_book_data.get('a', [])  # Bybit uses 'a' for asks

        if not bids or not asks:
            raise ValueError("Order book data is empty or invalid")

        # Convert to float and get best prices
        best_bid = float(bids[0][0])  # Highest bid price
        best_ask = float(asks[0][0])  # Lowest ask price

        # Calculate price ranges based on max allowed change
        price_change_factor = max_price_change_pct / 100

        # Calculate ask depth (liquidity for market sell orders)
        ask_depth_usdt = 0
        ask_levels = 0
        ask_weighted_price_sum = 0
        ask_total_quantity = 0
        ask_quantity = 0
        max_ask_price = best_ask * (1 + price_change_factor)

        for ask in asks:
            price = float(ask[0])
            quantity = float(ask[1])

            # Only include asks within our price tolerance
            if price <= max_ask_price:
                ask_depth_usdt += price * quantity
                ask_quantity += quantity
                ask_weighted_price_sum += price * quantity
                ask_total_quantity += quantity
                ask_levels += 1

        return {
            "symbol": symbol,
            "ask_depth_usdt": round(ask_depth_usdt, 2),
            "ask_quantity": ask_quantity,
            "category": category
        }

    async def get_wallet_balance(self, account_type: str = "UNIFIED", coin: str = None) -> dict:
        """
        Get wallet balance information from Bybit API

        Args:
            account_type (str): Account type ('UNIFIED', 'CONTRACT', 'SPOT', 'INVESTMENT', 'OPTION', 'FUND')
            coin (str, optional): Specific coin to query (e.g., 'USDT', 'BTC'). If None, returns all coins

        Returns:
            dict: Wallet balance information including:
                - account_type: Type of account queried
                - total_equity: Total account equity in USD
                - total_wallet_balance: Total wallet balance in USD
                - total_margin_balance: Total margin balance in USD
                - total_available_balance: Total available balance in USD
                - coin_balances: Dictionary of coin balances with detailed info
                - usdt_info: Specific USDT balance information (if available)
        """
        endpoint = "/v5/account/wallet-balance"

        params = {
            "accountType": account_type
        }

        if coin:
            params["coin"] = coin

        try:
            self.logger.debug(f"Fetching wallet balance for account type: {account_type}")

            data = await self._make_authenticated_request("GET", endpoint, params)
            result = data.get("result", {})

            if not result.get("list"):
                self.logger.warning("No wallet balance data found")
                return {
                    "account_type": account_type,
                    "total_equity": 0,
                    "total_wallet_balance": 0,
                    "total_margin_balance": 0,
                    "total_available_balance": 0,
                    "coin_balances": {},
                    "usdt_info": None
                }

            # Get the first account (should be the only one for the requested type)
            account_data = result["list"][0]

            # Extract account-level information
            total_equity = float(account_data.get("totalEquity", 0))
            total_wallet_balance = float(account_data.get("totalWalletBalance", 0))
            total_margin_balance = float(account_data.get("totalMarginBalance", 0))
            total_available_balance = float(account_data.get("totalAvailableBalance", 0))

            # Process coin balances
            coin_balances = {}
            usdt_info = None

            for coin_data in account_data.get("coin", []):
                coin_symbol = coin_data.get("coin", "")
                wallet_balance = float(coin_data.get("walletBalance", 0))
                available_balance = float(coin_data.get("availableToWithdraw", 0) or 0)
                equity = float(coin_data.get("equity", 0))
                locked = float(coin_data.get("locked", 0))

                # Only include coins with non-zero balances
                if wallet_balance > 0 or equity > 0:
                    coin_info = {
                        "coin": coin_symbol,
                        "wallet_balance": wallet_balance,
                        "available_balance": available_balance,
                        "equity": equity,
                        "locked": locked,
                        "unrealised_pnl": float(coin_data.get("unrealisedPnl", 0)),
                        "cum_realised_pnl": float(coin_data.get("cumRealisedPnl", 0)),
                        "available_to_withdraw": float(coin_data.get("availableToWithdraw", 0) or 0),
                        "margin_collateral": bool(coin_data.get("marginCollateral", False)),
                        "spot_hedging_qty": float(coin_data.get("spotHedgingQty", 0))
                    }

                    coin_balances[coin_symbol] = coin_info

                    # Store USDT-specific information
                    if coin_symbol == "USDT":
                        usdt_info = coin_info

            self.logger.info(f"Successfully retrieved wallet balance. Total equity: ${total_equity:.2f}")
            if usdt_info:
                self.logger.info(f"USDT available balance: ${usdt_info['available_balance']:.2f}")

            return {
                "account_type": account_type,
                "total_equity": round(total_equity, 2),
                "total_wallet_balance": round(total_wallet_balance, 2),
                "total_margin_balance": round(total_margin_balance, 2),
                "total_available_balance": round(total_available_balance, 2),
                "coin_balances": coin_balances,
                "usdt_info": usdt_info,
                "timestamp": int(time.time() * 1000)
            }

        except Exception as e:
            self.logger.error(f"Error fetching wallet balance: {str(e)}")
            raise

    async def get_usdt_balance(self, account_type: str = "UNIFIED") -> dict:
        """
        Convenience method to get USDT balance information

        Args:
            account_type (str): Account type ('UNIFIED', 'CONTRACT', 'SPOT', 'INVESTMENT', 'OPTION', 'FUND')

        Returns:
            dict: USDT balance information including:
                - available_usdt: Available USDT balance for trading
                - total_usdt: Total USDT wallet balance
                - locked_usdt: Locked USDT amount
                - equity_usdt: USDT equity
                - unrealised_pnl_usdt: Unrealised PNL in USDT
                - available_to_withdraw_usdt: USDT available for withdrawal
        """
        try:
            # Get wallet balance with USDT filter
            wallet_data = await self.get_wallet_balance(account_type=account_type, coin="USDT")

            usdt_info = wallet_data.get("usdt_info")

            if not usdt_info:
                self.logger.warning("No USDT balance found in wallet")
                return {
                    "available_usdt": 0,
                    "total_usdt": 0,
                    "locked_usdt": 0,
                    "equity_usdt": 0,
                    "unrealised_pnl_usdt": 0,
                    "available_to_withdraw_usdt": 0,
                    "account_type": account_type,
                    "timestamp": int(time.time() * 1000)
                }

            result = {
                "available_usdt": wallet_data["total_available_balance"],
                "total_usdt": usdt_info["wallet_balance"],
                "locked_usdt": usdt_info["locked"],
                "equity_usdt": usdt_info["equity"],
                "unrealised_pnl_usdt": usdt_info["unrealised_pnl"],
                "available_to_withdraw_usdt": usdt_info["available_to_withdraw"],
                "account_type": account_type,
                "timestamp": wallet_data["timestamp"]
            }

            self.logger.info(f"USDT Balance - Available: ${result['available_usdt']:.2f}, "
                             f"Total: ${result['total_usdt']:.2f}")

            return result

        except Exception as e:
            self.logger.error(f"Error fetching USDT balance: {str(e)}")
            raise

    async def get_position_info(self, symbol: str = None, category: str = "linear",
                                settlement_coin: str = None, limit: int = 200) -> dict:
        """
        Fetch position information from Bybit API with pagination support

        Args:
            symbol (str, optional): Trading pair symbol (e.g., 'BTCUSDT'). If None, fetches all positions
            category (str): Product category ('linear', 'inverse', 'option'). Default is 'linear'
            settlement_coin (str, optional): Settlement coin (e.g., 'USDT', 'USDC'). Only for linear category
            limit (int): Number of positions to retrieve per page (1-200). Default is 200

        Returns:
            dict: Position data including:
                - positions: List of all positions
                - total_positions: Total number of positions found
                - categories_processed: List of categories that were processed
        """
        endpoint = "/v5/position/list"

        # Build base parameters
        params = {
            "category": category,
            "limit": limit
        }

        # Add optional parameters
        if symbol:
            params["symbol"] = symbol
        if settlement_coin and category == "linear":
            params["settleCoin"] = settlement_coin

        all_positions = []
        next_page_cursor = None
        page_count = 0

        try:
            while True:
                # Add cursor for pagination if we have one
                if next_page_cursor:
                    params["cursor"] = next_page_cursor

                self.logger.debug(f"Fetching positions page {page_count + 1} with params: {params}")

                # Make authenticated request
                data = await self._make_authenticated_request("GET", endpoint, params)

                # Extract positions from response
                result = data.get("result", {})
                positions = result.get("list", [])

                if positions:
                    all_positions.extend(positions)
                    self.logger.debug(f"Retrieved {len(positions)} positions on page {page_count + 1}")

                # Check for next page
                next_page_cursor = result.get("nextPageCursor")
                page_count += 1

                # Break if no more pages or if we got less than the limit (last page)
                if not next_page_cursor or len(positions) < limit:
                    break

                # Safety check to prevent infinite loops
                if page_count > 100:  # Reasonable limit for API calls
                    self.logger.warning("Reached maximum page limit (100), stopping pagination")
                    break

        except Exception as e:
            self.logger.error(f"Error fetching position info: {str(e)}")
            raise

        # Filter positions if symbol was specified (additional client-side filtering)
        if symbol:
            filtered_positions = [pos for pos in all_positions if pos.get("symbol") == symbol]
            all_positions = filtered_positions

        self.logger.info(f"Successfully retrieved {len(all_positions)} positions across {page_count} pages")

        return {
            "positions": all_positions,
            "total_positions": len(all_positions),
            "pages_processed": page_count,
            "category": category,
            "symbol_filter": symbol,
            "settlement_coin_filter": settlement_coin
        }

    async def get_position_by_symbol(self, symbol: str, category: str = "linear") -> Union[dict, None]:
        """
        Convenience method to get position info for a specific symbol

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            category (str): Product category ('linear', 'inverse', 'option'). Default is 'linear'

        Returns:
            dict: Position data for the specific symbol, or None if position not found
        """
        try:
            result = await self.get_position_info(symbol=symbol, category=category)
            positions = result.get("positions", [])

            if positions:
                # Return the first (and should be only) position for this symbol
                position = positions[0]
                self.logger.info(f"Found position for {symbol}: {position.get('side', 'N/A')} "
                                 f"{position.get('size', '0')} @ {position.get('avgPrice', '0')}")
                return position
            else:
                self.logger.info(f"No position found for symbol {symbol}")
                return None

        except Exception as e:
            self.logger.error(f"Error fetching position for {symbol}: {str(e)}")
            raise

    async def set_leverage(
            self,
            symbol: str,
            buy_leverage: float,
            sell_leverage: float = None,
            category: str = "linear",
    ) -> dict:
        """
        Set leverage for a trading symbol

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            buy_leverage (float): Leverage for long positions (e.g., 15)
            sell_leverage (float, optional): Leverage for short positions. If None, uses buy_leverage
            category (str): Product category (default: 'linear')

        Returns:
            dict: API response from set leverage endpoint
        """
        if sell_leverage is None:
            sell_leverage = buy_leverage

        endpoint = "/v5/position/set-leverage"

        params = {
            "category": category,
            "symbol": symbol,
            "buyLeverage": str(buy_leverage),
            "sellLeverage": str(sell_leverage)
        }

        try:
            self.logger.info(f"Setting leverage for {symbol}: Buy={buy_leverage}x, Sell={sell_leverage}x")

            data = await self._make_authenticated_request("POST", endpoint, params)

            self.logger.info(f"Successfully set leverage for {symbol}")
            return data

        except Exception as e:
            self.logger.error(f"Error setting leverage for {symbol}: {str(e)}")
            raise

    async def get_closed_positions(self, symbol: str = None, category: str = "linear", limit: int = 50) -> dict:
        """
        Fetch closed positions (trading history) from Bybit API with ROI and P&L calculations

        Args:
            symbol (str, optional): Trading pair symbol (e.g., 'BTCUSDT'). If None, fetches all symbols
            category (str): Product category ('linear', 'inverse', 'option'). Default is 'linear'
            start_time (int, optional): Start timestamp in milliseconds
            end_time (int, optional): End timestamp in milliseconds
            limit (int): Number of records to retrieve (1-200). Default is 50

        Returns:
            dict: Closed positions data including:
                - closed_positions: List of closed positions with calculated ROI
                - total_closed_positions: Total number of closed positions
                - total_realized_pnl: Sum of all realized P&L
                - profitable_trades: Number of profitable trades
                - losing_trades: Number of losing trades
                - win_rate: Win rate percentage
                - average_roi: Average ROI across all trades
        """
        endpoint = "/v5/position/closed-pnl"

        # Build base parameters
        params = {
            "category": category,
            "limit": limit
        }

        # Add optional parameters
        if symbol:
            params["symbol"] = symbol

        all_closed_positions = []

        try:
            # Make authenticated request
            data = await self._make_authenticated_request("GET", endpoint, params)

            # Extract closed positions from response
            result = data.get("result", {})
            positions = result.get("list", [])

            all_closed_positions.extend(positions)

        except Exception as e:
            self.logger.error(f"Error fetching closed positions: {str(e)}")
            raise

        return {
            "closed_positions": all_closed_positions,
            "total_closed_positions": len(all_closed_positions),
            "category": category,
            "symbol_filter": symbol
        }

    async def set_trading_stop(
            self,
            symbol: str,
            category: str = "linear",
            position_idx: int = 0,
            take_profit: float = None,
            stop_loss: float = None,
            tp_trigger_by: str = None,
            sl_trigger_by: str = None,
            tp_size: float = None,
            sl_size: float = None,
            tp_limit_price: float = None,
            sl_limit_price: float = None,
            tp_order_type: str = None,
            sl_order_type: str = None
    ) -> dict:
        """
        Set trading stop for a position (take profit, stop loss)

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            category (str): Product category ('linear', 'inverse', 'option'). Default is 'linear'
            position_idx (int): Position index. 0: one-way mode, 1: hedge-mode Buy, 2: hedge-mode Sell
            take_profit (float, optional): Take profit price. Set to 0 to cancel existing TP
            stop_loss (float, optional): Stop loss price. Set to 0 to cancel existing SL
            tp_trigger_by (str, optional): TP trigger price type ('LastPrice', 'IndexPrice', 'MarkPrice')
            sl_trigger_by (str, optional): SL trigger price type ('LastPrice', 'IndexPrice', 'MarkPrice')
            tp_size (float, optional): TP size. Valid for Partial TP/SL
            sl_size (float, optional): SL size. Valid for Partial TP/SL
            tp_limit_price (float, optional): Limit price when TP is triggered (for limit orders)
            sl_limit_price (float, optional): Limit price when SL is triggered (for limit orders)
            tp_order_type (str, optional): TP order type ('Market', 'Limit')
            sl_order_type (str, optional): SL order type ('Market', 'Limit')

        Returns:
            dict: API response from set trading stop endpoint

        Raises:
            ValueError: If required parameters are missing or invalid
            Exception: If API request fails
        """
        endpoint = "/v5/position/trading-stop"

        # Build base parameters
        params = {
            "category": category,
            "symbol": symbol,
            "positionIdx": position_idx
        }

        # Add optional parameters only if they are not None
        if take_profit is not None:
            params["takeProfit"] = str(take_profit)
        if stop_loss is not None:
            params["stopLoss"] = str(stop_loss)
        if tp_trigger_by is not None:
            params["tpTriggerBy"] = tp_trigger_by
        if sl_trigger_by is not None:
            params["slTriggerBy"] = sl_trigger_by
        if tp_size is not None:
            params["tpSize"] = str(tp_size)
        if sl_size is not None:
            params["slSize"] = str(sl_size)
        if tp_limit_price is not None:
            params["tpLimitPrice"] = str(tp_limit_price)
        if sl_limit_price is not None:
            params["slLimitPrice"] = str(sl_limit_price)
        if tp_order_type is not None:
            params["tpOrderType"] = tp_order_type
        if sl_order_type is not None:
            params["slOrderType"] = sl_order_type

        # Validate trigger price types
        valid_trigger_types = ["LastPrice", "IndexPrice", "MarkPrice"]
        if tp_trigger_by and tp_trigger_by not in valid_trigger_types:
            raise ValueError(f"Invalid tp_trigger_by: {tp_trigger_by}. Must be one of {valid_trigger_types}")
        if sl_trigger_by and sl_trigger_by not in valid_trigger_types:
            raise ValueError(f"Invalid sl_trigger_by: {sl_trigger_by}. Must be one of {valid_trigger_types}")

        # Validate order types
        valid_order_types = ["Market", "Limit"]
        if tp_order_type and tp_order_type not in valid_order_types:
            raise ValueError(f"Invalid tp_order_type: {tp_order_type}. Must be one of {valid_order_types}")
        if sl_order_type and sl_order_type not in valid_order_types:
            raise ValueError(f"Invalid sl_order_type: {sl_order_type}. Must be one of {valid_order_types}")

        # Validate position index
        if position_idx not in [0, 1, 2]:
            raise ValueError("position_idx must be 0 (one-way), 1 (hedge-mode Buy), or 2 (hedge-mode Sell)")

        # Check if at least one trading stop parameter is provided
        if all(param is None for param in [take_profit, stop_loss]):
            raise ValueError("At least one of take_profit or stop_loss must be specified")

        try:
            # Build log message
            log_parts = []
            if take_profit is not None:
                if take_profit == 0:
                    log_parts.append("Canceling TP")
                else:
                    log_parts.append(f"TP: {take_profit}")
            if stop_loss is not None:
                if stop_loss == 0:
                    log_parts.append("Canceling SL")
                else:
                    log_parts.append(f"SL: {stop_loss}")

            self.logger.info(f"Setting trading stop for {symbol}: {', '.join(log_parts)}")

            # Make authenticated request
            data = await self._make_authenticated_request("POST", endpoint, params)

            self.logger.info(f"Successfully set trading stop for {symbol}")
            return data

        except Exception as e:
            self.logger.error(f"Error setting trading stop for {symbol}: {str(e)}")
            raise

    async def place_batch_market_orders(
            self,
            orders: list,
            category: str = "linear"
    ) -> dict:
        """
        Place a batch of market orders (up to 10 orders per batch)

        Args:
            orders (list): List of order dictionaries, each containing:
                - symbol (str): Trading pair symbol
                - side (str): Order side ('Buy' or 'Sell')
                - qty (float): Order quantity
                - position_idx (int, optional): Position index, default is 0
                - reduce_only (bool, optional): Whether the order is reduce-only, default is False
            category (str): Product category. Default is 'linear'

        Returns:
            dict: API response from batch order endpoint
        """
        if not orders:
            raise ValueError("Orders list cannot be empty")

        if len(orders) > 10:
            raise ValueError("Cannot process more than 10 orders in a single batch")

        endpoint = "/v5/order/create-batch"

        # Prepare request data
        request = {
            "category": category,
            "request": []
        }

        for order in orders:
            order_data = {
                "symbol": order["symbol"],
                "side": order["side"],
                "orderType": "Market",
                "qty": str(order["qty"]),
                "timeInForce": "GTC",
                "positionIdx": order.get("position_idx", 0),
                "reduceOnly": order.get("reduce_only", False)
            }
            request["request"].append(order_data)

        try:
            self.logger.info(f"Placing batch of {len(orders)} market orders")
            data = await self._make_authenticated_request("POST", endpoint, request)
            return data

        except Exception as e:
            self.logger.error(f"Error placing batch market orders: {str(e)}")
            raise

    async def open_position_with_market_orders(
            self,
            symbol: str,
            side: str,
            total_qty: float,
            category: str = "linear",
            position_idx: int = 0,
            leverage: float = None,
            reduce_only: bool = False,
    ) -> dict:
        """
        Open a position using multiple market orders if necessary, respecting limits

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            side (str): Order side ('Buy' or 'Sell')
            total_qty (float): Total position size to open
            category (str): Product category. Default is 'linear'
            position_idx (int): Position index. Default is 0 (one-way mode)
            leverage (float, optional): If provided, sets the leverage before placing orders

        Returns:
            dict: Summary of all executed orders
        """
        # Validate inputs
        if total_qty <= 0:
            raise ValueError("Total quantity must be greater than 0")

        # Set leverage if provided
        if leverage is not None:
            await self.set_leverage(symbol, leverage, leverage, category)

        # Get market limits for the symbol
        trading_params = await self.get_trading_params()

        # Extract the coin symbol from the trading pair (e.g., "BTC" from "BTCUSDT")
        coin_symbol = symbol.replace("USDT", "").replace("USDC", "")

        # Get limits for this coin
        coin_limits = trading_params.get(coin_symbol, {})

        if not coin_limits:
            self.logger.warning(f"No limits found for {coin_symbol}, using default values")
            market_order_limit = total_qty  # Default to total qty if no limits found
            min_order_size = 1.0
        else:
            market_order_limit = float(coin_limits.get("maxOrderQty", total_qty))
            min_order_size = float(coin_limits.get("minOrderQty", 1.0))

        self.logger.info(f"Market order limit for {symbol}: {market_order_limit}, min order size: {min_order_size}")

        # Calculate number of orders needed
        remaining_qty = total_qty
        orders_to_execute = []

        while remaining_qty > 0:
            # Calculate quantity for this order
            order_qty = min(remaining_qty, market_order_limit)

            # Ensure order meets minimum size
            if order_qty < min_order_size:
                if remaining_qty < min_order_size:
                    self.logger.warning(
                        f"Remaining quantity {remaining_qty} is below minimum order size {min_order_size}"
                    )
                    break
                order_qty = min_order_size

            # Add order to the list
            orders_to_execute.append({
                "symbol": symbol,
                "side": side,
                "qty": order_qty,
                "position_idx": position_idx,
                "reduce_only": reduce_only,
            })

            # Update remaining quantity
            remaining_qty -= order_qty

        # Process orders in batches of 10 with rate limiting
        results = []
        batches = [
            orders_to_execute[i:i + self.config.BOT_MAX_ORDERS_PER_SECOND]
            for i in range(0, len(orders_to_execute), self.config.BOT_MAX_ORDERS_PER_SECOND)
        ]

        for i in range(len(batches)):
            batches[i] = batches[i][::-1]

        self.logger.info(f"Executing {len(orders_to_execute)} orders in {len(batches)} batches")

        for i, batch in enumerate(batches):
            try:
                # Place batch order
                batch_result = await self.place_batch_market_orders(batch, category)
                results.append(batch_result)

                # Rate limiting: 1 request per second
                if i < len(batches) - 1:  # Don't wait after the last batch
                    self.logger.debug("Rate limiting: waiting 1 second before next batch")
                    await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in batch {i + 1}: {str(e)}")
                # Continue with remaining batches

        return {
            "symbol": symbol,
            "side": side,
            "total_qty_requested": total_qty,
            "total_qty_executed": total_qty - remaining_qty,
            "total_orders": len(orders_to_execute),
            "total_batches": len(batches),
            "results": results
        }

    async def close_position_with_market_orders(
            self,
            symbol: str,
            category: str = "linear",
            position_idx: int = 0,
            use_close_on_trigger: bool = True
    ) -> dict:
        """
        Close an existing position using market orders with proper batch handling for large positions

        Args:
            symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
            category (str): Product category. Default is 'linear'
            position_idx (int): Position index. Default is 0 (one-way mode)
            use_close_on_trigger (bool): Whether to use closeOnTrigger=true with qty="0" for positions
                                       within maxMktOrderQty limit. Default is True

        Returns:
            dict: Summary of the position closing operation
        """
        # Get current position info
        position = await self.get_position_by_symbol(symbol, category)

        if not position:
            self.logger.warning(f"No position found for {symbol}")
            return {"status": "no_position", "symbol": symbol}

        # Extract position details
        size = float(position.get("size", 0))
        side = position.get("side", "")

        if size == 0:
            self.logger.warning(f"Position size is 0 for {symbol}")
            return {"status": "zero_size", "symbol": symbol, "position": position}

        # Get trading parameters to check limits
        trading_params = await self.get_trading_params()
        coin_symbol = symbol.replace("USDT", "").replace("USDC", "")
        coin_limits = trading_params.get(coin_symbol, {})

        if not coin_limits:
            self.logger.warning(f"No limits found for {coin_symbol}, falling back to regular close method")
            # Fallback to the existing method
            return await self._close_position_regular_method(symbol, side, size, category, position_idx)

        max_market_order_qty = float(coin_limits.get("maxOrderQty", 0))

        self.logger.info(f"Closing {side} position of {size} {symbol}. Max market order qty: {max_market_order_qty}")

        # Determine close side (opposite of current position)
        close_side = "Sell" if side == "Buy" else "Buy"

        # Check if we can close the entire position with a single order using closeOnTrigger
        if use_close_on_trigger and size <= max_market_order_qty:
            return await self._close_position_single_order(symbol, close_side, category, position_idx, position, size)
        else:
            return await self._close_position_batch_orders(symbol, close_side, size, category, position_idx,
                                                           max_market_order_qty, position)

    async def _close_position_single_order(
            self,
            symbol: str,
            close_side: str,
            category: str,
            position_idx: int,
            position: dict,
            size: float
    ) -> dict:
        """
        Close position with a single order using closeOnTrigger=true and qty="0"
        """
        endpoint = "/v5/order/create"

        params = {
            "category": category,
            "symbol": symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": "0",  # Special value for closing entire position
            "positionIdx": position_idx,
            "reduceOnly": True,
            "closeOnTrigger": True,  # This allows closing up to maxMktOrderQty
            "timeInForce": "IOC"  # Immediate-or-Cancel for market orders
        }

        try:
            self.logger.info(
                f"Closing entire position of {size} {symbol} with single market order (closeOnTrigger=true)")

            result = await self._make_authenticated_request("POST", endpoint, params)

            return {
                "status": "success_single_order",
                "symbol": symbol,
                "method": "closeOnTrigger",
                "original_position": {
                    "side": position.get("side"),
                    "size": size
                },
                "close_side": close_side,
                "total_orders": 1,
                "result": result
            }

        except Exception as e:
            self.logger.error(f"Error closing position with single order for {symbol}: {str(e)}")
            # Fallback to batch method if single order fails
            self.logger.info(f"Falling back to batch method for {symbol}")
            return await self._close_position_batch_orders(
                symbol, close_side, size, "linear", position_idx,
                float("inf"), position  # Use infinity as fallback max qty
            )

    async def _close_position_batch_orders(
            self,
            symbol: str,
            close_side: str,
            total_size: float,
            category: str,
            position_idx: int,
            max_market_order_qty: float,
            position: dict
    ) -> dict:
        """
        Close position using multiple batch orders
        """
        self.logger.info(
            f"Closing position of {total_size} {symbol} using batch orders (max per order: {max_market_order_qty})")

        # Calculate orders needed
        remaining_qty = total_size
        orders_to_execute = []

        # Get minimum order size
        trading_params = await self.get_trading_params()
        coin_symbol = symbol.replace("USDT", "").replace("USDC", "")
        coin_limits = trading_params.get(coin_symbol, {})
        min_order_size = float(coin_limits.get("minOrderQty", 0.001))

        while remaining_qty > min_order_size:
            # Calculate quantity for this order
            order_qty = min(remaining_qty, max_market_order_qty)

            # Ensure we don't go below minimum order size
            if order_qty < min_order_size:
                break

            orders_to_execute.append({
                "symbol": symbol,
                "side": close_side,
                "qty": order_qty,
                "position_idx": position_idx,
                "reduce_only": True,
            })

            remaining_qty -= order_qty

        if not orders_to_execute:
            raise ValueError(
                f"Cannot create any valid orders. Remaining qty: {remaining_qty}, Min order size: {min_order_size}")

        # Process orders in batches
        results = []
        batches = [
            orders_to_execute[i:i + self.config.BOT_MAX_ORDERS_PER_SECOND]
            for i in range(0, len(orders_to_execute), self.config.BOT_MAX_ORDERS_PER_SECOND)
        ]

        self.logger.info(f"Executing {len(orders_to_execute)} close orders in {len(batches)} batches")

        for i, batch in enumerate(batches):
            try:
                batch_result = await self.place_batch_market_orders(batch, category)
                results.append(batch_result)

                # Rate limiting between batches
                if i < len(batches) - 1:
                    self.logger.debug("Rate limiting: waiting 1 second before next batch")
                    await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in close batch {i + 1}: {str(e)}")
                # Continue with remaining batches

        return {
            "status": "success_batch_orders",
            "symbol": symbol,
            "method": "batch_market_orders",
            "original_position": {
                "side": position.get("side"),
                "size": total_size
            },
            "close_side": close_side,
            "total_qty_requested": total_size,
            "total_qty_executed": total_size - remaining_qty,
            "remaining_qty": remaining_qty,
            "total_orders": len(orders_to_execute),
            "total_batches": len(batches),
            "results": results
        }

    async def _close_position_regular_method(
            self,
            symbol: str,
            side: str,
            size: float,
            category: str,
            position_idx: int
    ) -> dict:
        """
        Fallback method using the original approach when limits are not available
        """
        close_side = "Sell" if side == "Buy" else "Buy"

        self.logger.info(f"Using regular close method for {size} {symbol}")

        result = await self.open_position_with_market_orders(
            symbol=symbol,
            side=close_side,
            total_qty=size,
            category=category,
            position_idx=position_idx,
            reduce_only=True
        )

        result["status"] = "success_regular_method"
        result["method"] = "regular_market_orders"
        result["original_position"] = {
            "side": side,
            "size": size
        }

        return result

    async def get_executions(
            self,
            symbol: str,
            category: str = "linear",
            start_time: int = None,
            limit: int = 100,
    ) -> list[dict]:
        """
        Fetch recent executions (trade fills) for a symbol from Bybit.

        Args:
            symbol: Trading pair symbol (e.g. 'BTCUSDT').
            category: Product category. Default 'linear'.
            start_time: Optional lower bound in milliseconds.
            limit: Page size (max 100 for executions endpoint).

        Returns:
            List of execution dicts from result.list (empty list if none).
        """
        endpoint = "/v5/execution/list"
        params = {
            "category": category,
            "symbol": symbol,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = int(start_time)

        try:
            data = await self._make_authenticated_request("GET", endpoint, params)
            return data.get("result", {}).get("list", []) or []
        except Exception as e:
            self.logger.error(f"Error fetching executions for {symbol}: {str(e)}")
            raise

    async def get_most_recent_closed_position(self, symbol: str, time_window_ms: int = 60000) -> list[dict]:
        """
        Find the most recent closed position(s) for a given symbol.

        Args:
            symbol (str): The trading symbol to search for (e.g., "ZKUSDT")
            time_window_ms (int): Time window in milliseconds to group positions (default: 60000ms = 1 minute)

        Returns:
            list: List of position dictionaries that were closed together most recently,
                  or empty list if symbol not found
        """
        full_symbol = f"{symbol}USDT"
        positions = (await self.get_closed_positions(symbol=full_symbol, limit=200))["closed_positions"]

        if not positions:
            return []

        # Sort by updatedTime descending to ensure most recent first
        positions.sort(key=lambda x: int(x.get('updatedTime', 0)), reverse=True)

        # Get the most recent position's timestamp
        most_recent_time = int(positions[0].get('updatedTime', 0))

        # Find all positions within the time window of the most recent one
        grouped_positions = []
        for position in positions:
            position_time = int(position.get('updatedTime', 0))
            time_diff = abs(most_recent_time - position_time)

            if time_diff <= time_window_ms:
                grouped_positions.append(position)
            else:
                # Since data is sorted by time descending, we can break early
                break

        return grouped_positions


async def main():
    """Example usage of the async Bybit API client"""
    config = get_settings()
    client = BybitAPI(config)

    try:
        print((await client.get_trading_params())["SXT"])
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
