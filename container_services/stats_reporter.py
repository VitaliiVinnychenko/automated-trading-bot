import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

from core.config import get_settings
from core.logger import logger
from services.db_service import DbService
from services.third_party.telegram_api import TelegramNotifier
from utils.utils import seconds_to_days_hours

config = get_settings()
telegram_notifier = TelegramNotifier(logger=logger)


def _current_week_key() -> str:
    return datetime.now(timezone.utc).strftime("%G-W%V")


def _previous_week_key(current: str) -> str:
    year, week = current.split("-W")
    dt = datetime.strptime(f"{year} {week} 1", "%G %V %u")
    prev = dt - timedelta(weeks=1)
    return prev.strftime("%G-W%V")


def _current_month_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _previous_month_key(current: str) -> str:
    dt = datetime.strptime(current, "%Y-%m")
    first_of_month = dt.replace(day=1)
    prev = first_of_month - timedelta(days=1)
    return prev.strftime("%Y-%m")


def _format_month_display(month_key: str) -> str:
    dt = datetime.strptime(month_key, "%Y-%m")
    return dt.strftime("%B %Y")


def aggregate_period_stats(
    events: List[Dict[str, Any]],
    still_open: List[Dict[str, Any]],
) -> Dict[str, Any]:
    opened_events = [e for e in events if e["event_type"] == "opened"]
    closed_events = [e for e in events if e["event_type"] == "closed"]

    # Coerce None -> 0 so events with unknown stats (stats_source == "degraded"
    # or "ws_payload") don't crash aggregation.
    total_pnl = sum((e.get("pnl") or 0) for e in closed_events)
    total_fees = sum((e.get("total_fee") or 0) for e in closed_events)
    win_count = sum(1 for e in closed_events if e.get("pnl") is not None and e["pnl"] >= 0)
    loss_count = sum(1 for e in closed_events if e.get("pnl") is not None and e["pnl"] < 0)

    long_opened = sum(1 for e in opened_events if e.get("order_type") == "LONG")
    short_opened = sum(1 for e in opened_events if e.get("order_type") == "SHORT")

    long_still_open = sum(1 for o in still_open if o.get("order_type") == "LONG")
    short_still_open = sum(1 for o in still_open if o.get("order_type") == "SHORT")

    return {
        "total_opened": len(opened_events),
        "total_closed": len(closed_events),
        "still_open_count": len(still_open),
        "still_open": still_open,
        "long_opened": long_opened,
        "short_opened": short_opened,
        "long_still_open": long_still_open,
        "short_still_open": short_still_open,
        "total_pnl": round(total_pnl, 2),
        "total_fees": round(total_fees, 2),
        "win_count": win_count,
        "loss_count": loss_count,
        "symbols_opened": [e["symbol"] for e in opened_events],
        "closed_events": closed_events,
    }


def compare_periods(
    current: Dict[str, Any],
    previous: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if not previous:
        return {
            "opened_delta": None,
            "closed_delta": None,
            "still_open_delta": None,
            "pnl_delta": None,
            "prev_pnl": None,
            "prev_win_rate": None,
        }

    prev_total = previous["win_count"] + previous["loss_count"]
    prev_win_rate = (previous["win_count"] / prev_total * 100) if prev_total > 0 else 0

    return {
        "opened_delta": current["total_opened"] - previous["total_opened"],
        "closed_delta": current["total_closed"] - previous["total_closed"],
        "still_open_delta": current["still_open_count"] - previous["still_open_count"],
        "pnl_delta": round(current["total_pnl"] - previous["total_pnl"], 2),
        "prev_pnl": previous["total_pnl"],
        "prev_win_rate": round(prev_win_rate, 1),
    }


def _delta_str(value: Optional[int | float]) -> str:
    if value is None:
        return ""
    sign = "+" if value >= 0 else ""
    if isinstance(value, float):
        return f" ({sign}{value:,.2f})"
    return f" ({sign}{value})"


def _format_report(
    period_label: str,
    agg: Dict[str, Any],
    comp: Dict[str, Any],
) -> str:
    total_closed = agg["win_count"] + agg["loss_count"]
    win_rate = (agg["win_count"] / total_closed * 100) if total_closed > 0 else 0

    prev_pnl_str = ""
    if comp["prev_pnl"] is not None:
        prev_pnl_str = f" (prev: ${comp['prev_pnl']:+,.2f})"

    lines = [
        f"<strong>--- {period_label} ---</strong>",
        "",
        f"Opened: {agg['total_opened']}{_delta_str(comp['opened_delta'])}",
        f"Closed: {agg['total_closed']}{_delta_str(comp['closed_delta'])}",
        f"Still Open: {agg['still_open_count']}",
        f"  LONG: {agg['long_still_open']} | SHORT: {agg['short_still_open']}",
        "",
        f"Closed P&L: ${agg['total_pnl']:+,.2f}{prev_pnl_str}",
        f"Win Rate: {win_rate:.1f}% ({agg['win_count']}W / {agg['loss_count']}L)",
        f"Fees: ${agg['total_fees']:,.2f}",
    ]

    if agg["still_open"]:
        lines.append("")
        lines.append("<strong>Currently Open:</strong>")
        for pos in agg["still_open"]:
            age = time.time() - pos.get("created_at", time.time())
            lines.append(
                f"  - {pos['symbol']} {pos.get('order_type', '?')} ({seconds_to_days_hours(age)})"
            )

    if agg["closed_events"]:
        lines.append("")
        lines.append("<strong>Closed:</strong>")
        for ev in agg["closed_events"]:
            pnl = ev.get("pnl")
            pnl_str = f" (${pnl:+,.2f})" if pnl is not None else ""
            lines.append(f"  - {ev['symbol']}{pnl_str}")

    if agg["symbols_opened"]:
        lines.append("")
        lines.append(f"<strong>Opened:</strong> {', '.join(agg['symbols_opened'])}")

    return "\n".join(lines)


def format_weekly_report(
    agg: Dict[str, Any],
    comp: Dict[str, Any],
    week_key: str,
) -> str:
    return _format_report(f"Weekly Positions Report ({week_key})", agg, comp)


def format_monthly_report(
    agg: Dict[str, Any],
    comp: Dict[str, Any],
    month_key: str,
) -> str:
    label = _format_month_display(month_key)
    return _format_report(f"Monthly Positions Report ({label})", agg, comp)


async def _get_still_open_positions(db: DbService) -> List[Dict[str, Any]]:
    symbols = await db.get_active_orders()
    positions = []
    for symbol in symbols:
        order = await db.get_active_order_by_symbol(symbol)
        if order:
            order["symbol"] = symbol
            positions.append(order)
    return positions


async def _run_report(
    db: DbService,
    report_type: str,
    period_key: str,
    prev_period_key: str,
    event_prefix: str,
    format_fn,
) -> None:
    events = await db.get_stats_events(event_prefix, period_key)
    prev_events = await db.get_stats_events(event_prefix, prev_period_key)
    still_open = await _get_still_open_positions(db)

    current_agg = aggregate_period_stats(events, still_open)

    previous_agg = None
    if prev_events:
        previous_agg = aggregate_period_stats(prev_events, [])

    comp = compare_periods(current_agg, previous_agg)
    message = format_fn(current_agg, comp, period_key)

    await telegram_notifier.send_trade_update(message)
    await db.mark_stats_sent(report_type, period_key)

    logger.info(f"Sent {report_type} stats report for {period_key}")


async def main():
    db = DbService(config)
    report_hour = config.BOT_STATS_REPORT_HOUR_UTC

    logger.info(
        f"Stats reporter started. Report hour: {report_hour}:00 UTC. "
        f"Retention: {config.BOT_STATS_RETENTION_DAYS} days."
    )

    while True:
        try:
            now = datetime.now(timezone.utc)

            if now.hour == report_hour:
                # Weekly report on Monday: report on the week that just ended
                if now.weekday() == 0:
                    reported_week = _previous_week_key(_current_week_key())
                    if not await db.is_stats_sent("weekly", reported_week):
                        prev_week = _previous_week_key(reported_week)
                        await _run_report(
                            db, "weekly", reported_week, prev_week,
                            db.STATS_EVENT_WEEKLY_PREFIX,
                            format_weekly_report,
                        )

                # Monthly report on 1st: report on the month that just ended
                if now.day == 1:
                    reported_month = _previous_month_key(_current_month_key())
                    if not await db.is_stats_sent("monthly", reported_month):
                        prev_month = _previous_month_key(reported_month)
                        await _run_report(
                            db, "monthly", reported_month, prev_month,
                            db.STATS_EVENT_MONTHLY_PREFIX,
                            format_monthly_report,
                        )

        except Exception as e:
            logger.error(f"Error in stats reporter loop: {e}")

        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
