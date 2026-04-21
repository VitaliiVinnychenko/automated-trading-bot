import math

from core.enums import OrderType


def format_crypto_symbol(symbol: str) -> str:
    return symbol.upper().replace("/USDT", "").replace("USDT", "")


def round_by_min_order_size(value: float, min_order_size: float) -> float:
    """
    Round a float value based on the minimum order size precision.

    Args:
        value: The float value to round
        min_order_size: The minimum order size that determines precision

    Returns:
        The rounded float value

    Examples:
        round_by_min_order_size(1.23456, 0.001) -> 1.235 (3 decimal places)
        round_by_min_order_size(1.23456, 1.0) -> 1.0 (0 decimal places)
        round_by_min_order_size(1.23456, 0.1) -> 1.2 (1 decimal place)
    """
    if min_order_size >= 1.0:
        decimal_places = 0
    else:
        # Calculate decimal places based on min_order_size
        # For 0.001 -> 3 places, 0.01 -> 2 places, 0.1 -> 1 place
        decimal_places = -int(math.floor(math.log10(min_order_size)))

    return round(value, decimal_places)


async def calculate_closed_position_stats(positions, position_value) -> dict[str, float] | None:
    if not positions:
        return None

    total_exit_value = 0.0
    total_quantity = 0.0
    total_fee = 0.0
    total_pnl = 0.0
    total_cum_exit_value = 0.0
    total_cum_entry_value = 0.0

    for position in positions:
        try:
            exit_value = float(position.get('cumExitValue', 0))
            quantity = float(position.get('closedSize', 0))
            open_fee = float(position.get("openFee", 0))
            close_fee = float(position.get("closeFee", 0))
            pnl = float(position.get('closedPnl', 0))
            cum_entry_value = float(position.get("cumEntryValue", 0))
            cum_exit_value = float(position.get("cumExitValue", 0))

            total_exit_value += exit_value
            total_quantity += quantity
            total_fee += open_fee + close_fee
            total_pnl += pnl
            total_cum_entry_value += cum_entry_value
            total_cum_exit_value += cum_exit_value

        except (ValueError, TypeError):
            continue

    if total_quantity == 0:
        return None

    pnl_pct = round(total_pnl / position_value * 100, 2) if position_value else 0.0

    return {
        "average_exit_price": total_exit_value / total_quantity,
        "total_quantity": total_quantity,
        "total_fee": round(total_fee, 2),
        "pnl": round(total_pnl, 2),
        "pnl_pct": pnl_pct,
        "cum_entry_value": total_cum_entry_value,
        "cum_exit_value": total_cum_exit_value,
    }


def build_stats_from_executions(
        executions: list[dict],
        order: dict,
        leverage: float,
) -> dict | None:
    """
    Reconstruct close stats from /v5/execution/list entries.

    Filters to executions on the closing side for this position (Sell for LONG,
    Buy for SHORT) and only those that happened after the position was opened.
    Returns None if no matching executions are found.
    """
    if not executions:
        return None

    order_type = order.get("order_type")
    closing_side = "Sell" if order_type == OrderType.LONG else "Buy"
    created_at_ms = int(float(order.get("created_at", 0)) * 1000)
    avg_entry = float(order.get("average_price", 0) or 0)

    total_qty = 0.0
    total_exit_value = 0.0
    total_fee = 0.0

    for ex in executions:
        try:
            if ex.get("side") != closing_side:
                continue
            exec_time_ms = int(ex.get("execTime", 0) or 0)
            if created_at_ms and exec_time_ms < created_at_ms:
                continue
            qty = float(ex.get("execQty", 0) or 0)
            price = float(ex.get("execPrice", 0) or 0)
            fee = float(ex.get("execFee", 0) or 0)
            if qty <= 0 or price <= 0:
                continue
            total_qty += qty
            total_exit_value += qty * price
            total_fee += fee
        except (ValueError, TypeError):
            continue

    if total_qty == 0:
        return None

    average_exit_price = total_exit_value / total_qty

    if order_type == OrderType.LONG:
        pnl = (average_exit_price - avg_entry) * total_qty - total_fee
    else:
        pnl = (avg_entry - average_exit_price) * total_qty - total_fee

    order_value = (total_qty * avg_entry / leverage) if (avg_entry and leverage) else 0.0
    pnl_pct = round(pnl / order_value * 100, 2) if order_value else 0.0

    return {
        "average_exit_price": average_exit_price,
        "total_quantity": total_qty,
        "total_fee": round(total_fee, 2),
        "pnl": round(pnl, 2),
        "pnl_pct": pnl_pct,
    }


def build_stats_from_ws_payload(
        closure: dict,
        old_tracked: dict,
        order: dict,
        leverage: float,
) -> dict | None:
    """
    Build best-effort close stats from the WebSocket position update payload.

    Uses pre-close tracked size for quantity, curRealisedPnl for realized P&L of
    the just-closed round, and cumExitValue for average exit price. Fees are
    unavailable from WS and will be set to None. Returns None if neither P&L nor
    exit price can be derived.
    """
    try:
        total_quantity = abs(float(
            old_tracked.get("size") or order.get("quantity", 0) or 0
        ))
    except (ValueError, TypeError):
        total_quantity = 0.0

    try:
        pnl = float(closure.get("curRealisedPnl", 0) or 0)
    except (ValueError, TypeError):
        pnl = 0.0

    average_exit_price = None
    try:
        cum_exit_value = float(closure.get("cumExitValue", 0) or 0)
        if cum_exit_value == 0:
            # Fallback to pre-close positionValue which equals size * markPrice
            cum_exit_value = float(old_tracked.get("positionValue", 0) or 0)
        if cum_exit_value > 0 and total_quantity > 0:
            average_exit_price = cum_exit_value / total_quantity
    except (ValueError, TypeError):
        average_exit_price = None

    if pnl == 0 and average_exit_price is None:
        return None

    avg_entry = float(order.get("average_price", 0) or 0)
    order_value = (total_quantity * avg_entry / leverage) if (avg_entry and leverage) else 0.0
    pnl_pct = round(pnl / order_value * 100, 2) if order_value else 0.0

    return {
        "average_exit_price": average_exit_price,
        "total_quantity": total_quantity,
        "total_fee": None,
        "pnl": round(pnl, 2),
        "pnl_pct": pnl_pct,
    }


def seconds_to_days_hours(seconds):
    days = seconds // (24 * 3600)
    remaining_seconds = seconds % (24 * 3600)
    hours = remaining_seconds // 3600

    if days > 0 and hours > 0:
        return f"{days} days, {hours} hours"
    elif days > 0:
        return f"{days} days"
    elif hours > 0:
        return f"{hours} hours"
    else:
        return "Less than 1 hour"
