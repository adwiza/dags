from datetime import datetime, timedelta

from rd_chassis.misc.helpers import now_utc, est_timezone

from misc.tables.c4s_events import c4s_events_columns
from misc.tables.cb_transactions import cb_transactions_columns
from misc.tables.clipcash_deposits import clipcash_deposits_columns
from misc.tables.lander_events import lander_events_columns
from misc.tables.purchases import purchases_columns
from misc.tables.transactions import transactions_columns
from misc.tables.tributes import tributes_columns


def get_date_from(index: str) -> datetime:
    if index == "purchases":
        return now_utc() - timedelta(days=365 * 6)

    if index == "c4s-events":
        return (now_utc() - timedelta(days=180)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if index == "lander-events":
        return (now_utc() - timedelta(days=120)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if index == "transactions":
        return datetime(2012, 1, 1, 0, 0, 0, 0, est_timezone)

    if index == "clipcash-deposits":
        return datetime(2023, 4, 12, 0, 0, 0, 0, est_timezone)

    return now_utc() - timedelta(days=365)


def get_columns(table_name: str) -> list | None:
    if table_name == "c4s_events":
        return c4s_events_columns

    if table_name == "cb_transactions":
        return cb_transactions_columns

    if table_name == "clipcash_deposits":
        return clipcash_deposits_columns

    if table_name == "lander_events":
        return lander_events_columns

    if table_name == "purchases":
        return purchases_columns

    if table_name == "transactions":
        return transactions_columns

    if table_name == "tributes":
        return tributes_columns

    return None
