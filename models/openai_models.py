from typing import Literal

from pydantic import BaseModel, Field, model_validator


class Message(BaseModel):
    message_type: Literal[
        "CREATE_ORDER", "MARKET_UPDATE", "CLOSE_ORDER", "TAKE_PROFIT", "SIMPLE_MESSAGE", "ORDER_UPDATE"]
    symbol: str | None = Field(..., description="Cryptocurrency symbol like BTC, ETH, etc.")
    order_type: Literal["LONG", "SHORT"] | None
    entry_type: Literal["MARKET_PRICE", "LIMIT_ORDER"] | None
    dca: float | None = Field(..., description="Dollar cost average value")
    stop_loss: float | None = Field(..., description="Stop loss value of the order")
    entry_price: float | None = Field(..., description="Entry price of market order or limit order.")
    target_price: float | None = Field(..., description="The target price if provided")
    order_rating: int | None = Field(..., description="Rating of trade if provided. Without '/10' part")
    min_leverage: int | None = Field(..., description="Lower value of the suggested leverage for the order")
    max_leverage: int | None = Field(..., description="Higher value of the suggested leverage for the order")
    reason: str | None = Field(..., description="The reasoning of the order")

    @model_validator(mode='before')
    @classmethod
    def set_defaults_for_non_create_order(cls, values):
        if isinstance(values, dict):
            message_type = values.get('message_type')

            if message_type in ("MARKET_UPDATE", "SIMPLE_MESSAGE"):
                fields_to_default = [
                    'symbol', 'order_type', 'entry_type', 'dca', 'stop_loss',
                    'entry_price', 'target_price', 'order_rating',
                    'min_leverage', 'max_leverage', 'reason'
                ]

                for field in fields_to_default:
                    if field not in values or values.get(field) is ...:  # ... is Ellipsis (Field(...))
                        values[field] = None

            elif message_type in ("CLOSE_ORDER", "TAKE_PROFIT", "ORDER_UPDATE"):
                fields_to_default = [
                    'order_type', 'entry_type', 'dca', 'stop_loss',
                    'entry_price', 'target_price', 'order_rating',
                    'min_leverage', 'max_leverage', 'reason'
                ]

                for field in fields_to_default:
                    if field not in values or values.get(field) is ...:  # ... is Ellipsis (Field(...))
                        values[field] = None

        return values
