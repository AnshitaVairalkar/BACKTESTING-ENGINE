import pandas as pd
from datetime import time

from data.index_reader import read_index_data
from data.market_calendar import get_market_context
from data.options_reader import load_option_data
from engine.execution import execute_option_leg


def run_event_backtest(
    trade_date: str,
    index: str,
    index_parquet_map: dict,
    calendar_csv: str,
    options_parquet_root: str,
    strategy
):
    """
    Generic event-driven backtest engine.
    Supports BOTH:
    - Simple strategies (ITMStraddle)
    - Stateful strategies (dynamic inventory)
    """

    trades = []
    market = get_market_context(calendar_csv, trade_date)

    # ----------------------------
    # Load index data
    # ----------------------------
    index_df = read_index_data(index_parquet_map[index], trade_date)

    # ----------------------------
    # Strategy initialization
    # ----------------------------
    strategy.on_day_start(
        trade_date=trade_date,
        index=index,
        market_context=market
    )

    # ----------------------------
    # Minute-by-minute loop
    # ----------------------------
    for ts, row in index_df.iterrows():
        current_time = ts.time()

        # Stop after exit time
        if current_time > strategy.EXIT_TIME:
            break

        actions = strategy.on_minute(
            timestamp=ts,
            index_price=row["Close"]
        )

        for action in actions:
            if action["action"] == "ENTER":
                opt_df = load_option_data(
                    parquet_root=options_parquet_root,
                    trade_date=trade_date,
                    expiry=market["weekly_expiry"],
                    strike=action["strike"],
                    option_type=action["option_type"]
                )

                exec_result = execute_option_leg(
                    df=opt_df,
                    intended_entry_time=current_time,
                    exit_time=strategy.EXIT_TIME,
                    sl_pct=strategy.SL_PCT,
                    qty=action["qty"]
                )

                trades.append({
                    "DATE": trade_date,
                    "INDEX": index,
                    "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "DAY": market["day"],
                    "RANGE_USED": action["range_used"],
                    "REF_PRICE": action["ref_price"],
                    "UPPER_RANGE": action["upper"],
                    "LOWER_RANGE": action["lower"],
                    "ENTRY_TIME": exec_result["entry_time"],
                    "EXIT_TIME": exec_result["exit_time"],
                    "INDEX_ENTRY": action["index_entry"],
                    "INDEX_EXIT": None,
                    "STRIKE": action["strike"],
                    "TYPE": action["option_type"],
                    "ENTRY_PRICE": exec_result["entry_price"],
                    "EXIT_PRICE": exec_result["exit_price"],
                    "QTY": action["qty"],
                    "PNL": exec_result["pnl"],
                    "EXIT_REASON": exec_result["exit_reason"],
                })

            elif action["action"] == "EXIT":
                # EXIT handled logically — execution engine already exits legs
                pass

    # ----------------------------
    # End of day square-off
    # ----------------------------
    strategy.on_day_end()

    return trades
