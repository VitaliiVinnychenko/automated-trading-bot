from enum import StrEnum


class EntryType(StrEnum):
    LIMIT_ORDER = "LIMIT_ORDER"
    MARKET_PRICE = "MARKET_PRICE"


class OrderType(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class MessageType(StrEnum):
    CREATE_ORDER = "CREATE_ORDER"
    MARKET_UPDATE = "MARKET_UPDATE"
    CLOSE_ORDER = "CLOSE_ORDER"
    TAKE_PROFIT = "TAKE_PROFIT"
    SIMPLE_MESSAGE = "SIMPLE_MESSAGE"
    ORDER_UPDATE = "ORDER_UPDATE"
