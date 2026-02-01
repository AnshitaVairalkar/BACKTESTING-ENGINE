import pandas as pd
from datetime import datetime, time


def _to_time(t):
    """Convert various time formats to datetime.time object"""
    if isinstance(t, time):
        return t
    if isinstance(t, str):
        return datetime.strptime(t, "%H:%M").time()
    raise TypeError(f"Invalid time format: {t}")


def execute_option_leg(
    df: pd.DataFrame,
    intended_entry_time,
    exit_time,
    sl_pct: float,
    qty: int = -1
):
    """
    Execute one option leg with entry/exit logic and stop-loss.
    
    Args:
        df: Option data with DateTime index and OHLC columns
        intended_entry_time: Intended entry time (HH:MM or time object)
        exit_time: Exit time (HH:MM or time object)
        sl_pct: Stop loss percentage (e.g., 0.40 for 40%)
        qty: Quantity (+1 for buy, -1 for sell/short)
        
    Returns:
        Dictionary with execution details including PnL
    """
    if df.empty:
        raise ValueError("Option dataframe is empty")

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Option dataframe index must be DatetimeIndex")

    entry_time = _to_time(intended_entry_time)
    exit_time = _to_time(exit_time)

    # -------------------------------------------------
    # ENTRY LOGIC
    # -------------------------------------------------
    eligible = df[df.index.time >= entry_time]

    if eligible.empty:
        raise ValueError("No candles available after intended entry time")

    entry_row = eligible.iloc[0]
    actual_entry_time = entry_row.name
    entry_price = entry_row["Open"]

    entry_delayed = actual_entry_time.time() != entry_time
    entry_delay_minutes = (
        int((actual_entry_time - actual_entry_time.replace(
            hour=entry_time.hour,
            minute=entry_time.minute,
            second=0,
            microsecond=0
        )).total_seconds() // 60)
        if entry_delayed else 0
    )

    # -------------------------------------------------
    # STOP LOSS CALCULATION
    # -------------------------------------------------
    # For short positions (qty=-1): SL triggers when price goes UP
    # For long positions (qty=+1): SL triggers when price goes DOWN
    if qty < 0:  # Short position
        sl_price = entry_price * (1 + sl_pct)
        sl_check = lambda row: row["High"] >= sl_price
    else:  # Long position
        sl_price = entry_price * (1 - sl_pct)
        sl_check = lambda row: row["Low"] <= sl_price

    exit_price = None
    exit_ts = None
    exit_reason = "TIME_EXIT"

    # -------------------------------------------------
    # STOP LOSS MONITORING
    # -------------------------------------------------
    for ts, row in eligible.iterrows():
        if ts.time() > exit_time:
            break

        if sl_check(row):
            # SL hit - exit at SL price
            if qty < 0:
                exit_price = row["High"]  # Short exits at high
            else:
                exit_price = row["Low"]   # Long exits at low
            exit_ts = ts
            exit_reason = "SL_HIT"
            break

    # -------------------------------------------------
    # TIME EXIT (if SL not hit)
    # -------------------------------------------------
    if exit_price is None:
        exit_row = eligible[eligible.index.time <= exit_time]

        if exit_row.empty:
            raise ValueError("No candle available before exit time")

        last_row = exit_row.iloc[-1]
        exit_price = last_row["Close"]
        exit_ts = last_row.name

    # -------------------------------------------------
    # P&L CALCULATION
    # -------------------------------------------------
    # For short (qty=-1): PnL = (Entry - Exit) * |qty|
    # For long (qty=+1): PnL = (Exit - Entry) * |qty|
    pnl = (exit_price - entry_price) * qty

    # -------------------------------------------------
    # RESULT
    # -------------------------------------------------
    return {
        "intended_entry_time": entry_time.strftime("%H:%M"),
        "entry_time": actual_entry_time,
        "entry_price": float(entry_price),
        "entry_delayed": entry_delayed,
        "entry_delay_minutes": entry_delay_minutes,
        
        "exit_time": exit_ts,
        "exit_price": float(exit_price),
        "exit_reason": exit_reason,
        "sl_price": float(sl_price),
        
        "qty": qty,
        "pnl": float(pnl),
    }