"""
Event Backtest Engine with Safe Candle Handling + Warning Tracking

Fixes:
- Handles missing candles on ALL days (not just expiry)
- Safe fallback when exact time not found
- Returns warnings list for logging to errors file
- Better error messages for debugging
"""

from pathlib import Path
from datetime import datetime, timedelta
from data.index_reader import read_index_data
from data.market_calendar import get_market_context
from data.options_reader import load_option_data
from engine.minute_pnl_tracker import MinutePnLTracker


def _safe_get_candle(opt_df, target_time, fallback="last"):
    """
    Safely get a candle at target_time with fallback options.
    
    Args:
        opt_df: DataFrame with DateTime index
        target_time: datetime.time to look for
        fallback: "last" = use last available candle
                  "nearest" = use nearest time
                  "none" = return None if not found
    
    Returns:
        (candle_series, actual_time, warning_msg) 
        warning_msg is None if exact match found
    """
    # Try exact match first
    exact_match = opt_df.loc[opt_df.index.time == target_time]
    
    if not exact_match.empty:
        return exact_match.iloc[0], target_time, None  # No warning
    
    # Fallback strategies
    warning_msg = None
    
    if fallback == "last":
        # Get last candle at or before target time
        before_target = opt_df[opt_df.index.time <= target_time]
        if not before_target.empty:
            candle = before_target.iloc[-1]
            actual_time = candle.name.time()
            warning_msg = f"Candle {target_time} not found, used {actual_time} (last before target)"
            return candle, actual_time, warning_msg
        
        # Absolute last resort: last candle in data
        if not opt_df.empty:
            candle = opt_df.iloc[-1]
            actual_time = candle.name.time()
            warning_msg = f"Candle {target_time} not found, used {actual_time} (last available)"
            return candle, actual_time, warning_msg
    
    elif fallback == "nearest":
        if not opt_df.empty:
            # Find nearest time
            time_diffs = opt_df.index.map(
                lambda x: abs(
                    (x.hour * 60 + x.minute) - 
                    (target_time.hour * 60 + target_time.minute)
                )
            )
            nearest_idx = time_diffs.argmin()
            candle = opt_df.iloc[nearest_idx]
            actual_time = candle.name.time()
            warning_msg = f"Candle {target_time} not found, used {actual_time} (nearest)"
            return candle, actual_time, warning_msg
    
    return None, None, f"Candle {target_time} not found and no fallback available"


def run_event_backtest_v2(
    trade_date: str,
    index: str,
    index_parquet_map: dict,
    calendar_csv: str,
    options_parquet_root: str,
    strategy,
    minute_pnl_tracker: "MinutePnLTracker" = None
):
    """
    Version 2 of event backtest engine with improved entry/exit logic.
    
    KEY DIFFERENCES FROM V1:
    - Uses CLOSE for breach detection (vs OPEN)
    - Exits on CLOSE when SL hit (vs OPEN)
    - Re-entries on NEXT candle's OPEN (vs same candle)
    - EOD exits on CLOSE (vs OPEN)
    
    ✅ FIXES:
    - Safe handling when exact candle time not found (ALL days)
    - Fallback to last available candle
    - Returns (trades, warnings) tuple for error logging
    
    Returns:
        tuple: (trades_list, warnings_list)
    """
    trades = []
    warnings = []

    # -------------------------------------------------
    # Market context
    # -------------------------------------------------
    market = get_market_context(calendar_csv, trade_date)

    index_df = read_index_data(
        index_parquet_map[index],
        trade_date
    )

    strategy.on_day_start(
        trade_date=trade_date,
        index=index,
        market_context=market
    )


    if minute_pnl_tracker is not None:
        minute_pnl_tracker.new_day(trade_date, strategy.get_strategy_name())
    open_legs = {}
    pending_entries = []  # Entries that should happen on NEXT candle's OPEN

    # -------------------------------------------------
    # INTRADAY LOOP (STRICTLY < EXIT_TIME)
    # -------------------------------------------------
    for ts, row in index_df.iterrows():
        candle_time = ts.time()

        if candle_time >= strategy.EXIT_TIME:
            break

        # 🔑 INDEX PRICE LOGIC:
        # - At ENTRY_TIME (9:20): Use OPEN for initial entry
        # - After ENTRY_TIME: Use CLOSE for breach detection
        if candle_time == strategy.ENTRY_TIME:
            index_price = row["Open"]
        else:
            index_price = row["Close"]

        # ================= PROCESS PENDING ENTRIES =================
        # These are entries that should happen on THIS candle's OPEN
        for pending in pending_entries:
            opt_df = load_option_data(
                parquet_root=options_parquet_root,
                trade_date=trade_date,
                expiry=market["weekly_expiry"],
                strike=pending["strike"],
                option_type=pending["type"]
            )

            # ✅ SAFE: Get candle with fallback
            candle, actual_time, warning_msg = _safe_get_candle(opt_df, candle_time, fallback="last")
            
            if candle is None:
                warnings.append({
                    "DATE": trade_date,
                    "INDEX": index,
                    "EXPIRY": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "ACTION": "PENDING_ENTRY",
                    "STRIKE": pending["strike"],
                    "TYPE": pending["type"],
                    "REQUESTED_TIME": str(candle_time),
                    "ACTUAL_TIME": "N/A",
                    "WARNING": "No candle for pending entry - skipped"
                })
                continue
            
            if warning_msg:
                warnings.append({
                    "DATE": trade_date,
                    "INDEX": index,
                    "EXPIRY": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "ACTION": "PENDING_ENTRY",
                    "STRIKE": pending["strike"],
                    "TYPE": pending["type"],
                    "REQUESTED_TIME": str(candle_time),
                    "ACTUAL_TIME": str(actual_time),
                    "WARNING": warning_msg
                })

            open_legs[pending["leg_id"]] = {
                "meta": pending,
                "entry_price": candle["Open"],  # ENTER ON OPEN
                "entry_time": actual_time,
                "opt_df": opt_df  # cached — avoids reload in tracker
            }

        pending_entries.clear()

        # ================= GET STRATEGY ACTIONS =================
        actions = strategy.on_minute(ts, index_price)

        for a in actions:

            # ================= ENTRY =================
            if a["action"] == "ENTER":
                # Check if this is FIRST entry (at ENTRY_TIME)
                if candle_time == strategy.ENTRY_TIME:
                    # First entry: enter immediately on this candle's OPEN
                    opt_df = load_option_data(
                        parquet_root=options_parquet_root,
                        trade_date=trade_date,
                        expiry=market["weekly_expiry"],
                        strike=a["strike"],
                        option_type=a["type"]
                    )

                    # ✅ SAFE: Get candle with fallback
                    candle, actual_time, warning_msg = _safe_get_candle(opt_df, candle_time, fallback="last")
                    
                    if candle is None:
                        raise ValueError(
                            f"No candle for ENTRY: {trade_date} | "
                            f"{a['type']} {a['strike']} | Time: {candle_time}"
                        )
                    
                    if warning_msg:
                        warnings.append({
                            "DATE": trade_date,
                            "INDEX": index,
                            "EXPIRY": market["weekly_expiry"].strftime("%Y-%m-%d"),
                            "ACTION": "ENTRY",
                            "STRIKE": a["strike"],
                            "TYPE": a["type"],
                            "REQUESTED_TIME": str(candle_time),
                            "ACTUAL_TIME": str(actual_time),
                            "WARNING": warning_msg
                        })

                    open_legs[a["leg_id"]] = {
                        "meta": a,
                        "entry_price": candle["Open"],
                        "entry_time": actual_time,
                        "opt_df": opt_df  # cached — avoids reload in tracker
                    }
                else:
                    # Subsequent entries: enter on NEXT candle's OPEN
                    pending_entries.append(a)

            # ================= EXIT =================
            elif a["action"] == "EXIT":
                leg = open_legs.pop(a["leg_id"])

                opt_df = load_option_data(
                    parquet_root=options_parquet_root,
                    trade_date=trade_date,
                    expiry=market["weekly_expiry"],
                    strike=leg["meta"]["strike"],
                    option_type=leg["meta"]["type"]
                )

                # ✅ SAFE: Get candle with fallback
                candle, actual_time, warning_msg = _safe_get_candle(opt_df, candle_time, fallback="last")
                
                if candle is None:
                    raise ValueError(
                        f"No candle for EXIT: {trade_date} | "
                        f"{leg['meta']['type']} {leg['meta']['strike']} | Time: {candle_time}"
                    )
                
                if warning_msg:
                    warnings.append({
                        "DATE": trade_date,
                        "INDEX": index,
                        "EXPIRY": market["weekly_expiry"].strftime("%Y-%m-%d"),
                        "ACTION": "EXIT",
                        "STRIKE": leg["meta"]["strike"],
                        "TYPE": leg["meta"]["type"],
                        "REQUESTED_TIME": str(candle_time),
                        "ACTUAL_TIME": str(actual_time),
                        "WARNING": warning_msg,
                        "EXIT_REASON": a["reason"]
                    })

                # EXIT ON CLOSE (when breach detected)
                exit_price = candle["Close"]
               
                pnl = (exit_price - leg["entry_price"]) * -1
                

                if minute_pnl_tracker is not None:
                    minute_pnl_tracker.add_realized(pnl)
                # Extract metadata for tradesheet
                meta = leg["meta"]
                
                # For volatility strategy - extract SL and volatility info
                sl_before_round = meta.get("sl_before_round", None)
                sl_index = meta.get("sl_index", None)
                volatility = meta.get("volatility", None)
                entry_index_price = meta.get("entry_index_price", None)
                
                # For inventory strategy - extract range info
                upper_range = meta.get("upper", None)
                lower_range = meta.get("lower", None)
                range_used = meta.get("R", None)
                ref_price = meta.get("ref_price", None)

                trades.append({
                    "DATE": trade_date,
                    "INDEX": index,
                    "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "DAY": market["day"],
                    
                    # Entry info
                    "ENTRY_TIME": leg["entry_time"].strftime("%H:%M") if hasattr(leg["entry_time"], 'strftime') else str(leg["entry_time"])[:5],
                    "INDEX_ENTRY_PRICE": entry_index_price if entry_index_price is not None else ref_price,
                    "ENTRY_PRICE": leg["entry_price"],
                    
                    # Exit info
                    "EXIT_TIME": actual_time.strftime("%H:%M") if hasattr(actual_time, 'strftime') else str(actual_time)[:5],
                    "INDEX_EXIT_PRICE": index_price,  # Index CLOSE when SL hit
                    "EXIT_PRICE": exit_price,  # Option CLOSE
                    "EXIT_REASON": a["reason"],
                    
                    # Strike and type
                    "STRIKE": meta["strike"],
                    "TYPE": meta["type"],
                    
                    # Strategy-specific fields (volatility strategy)
                    "SL_INDEX": sl_index,
                    "SL_BEFORE_ROUND": sl_before_round,
                    "VOLATILITY": volatility,
                    
                    # Strategy-specific fields (inventory strategy)
                    "UPPER_RANGE": upper_range,
                    "LOWER_RANGE": lower_range,
                    "RANGE_USED": range_used,
                    
                    # PnL
                    "QTY": -1,
                    "PNL": pnl,
                })

        # ================= 1-MIN PNL SNAPSHOT =================
        if minute_pnl_tracker is not None and open_legs:
            minute_pnl_tracker.record(
                ts=ts,
                trade_date=trade_date,
                open_legs=open_legs,
                market=market,
                expiry_date=market["weekly_expiry"].date()
            )

    # -------------------------------------------------
    # 🔒 EOD EXIT — WITH SAFE FALLBACK (CLOSE PRICE)
    # Uses EXIT_TIME - 1min so tradesheet PnL matches 1min PnL tracker
    # -------------------------------------------------
    _exit_dt = datetime.combine(datetime.today(), strategy.EXIT_TIME) - timedelta(minutes=1)
    eod_time = _exit_dt.time()

    for leg_id, leg in open_legs.items():
        opt_df = load_option_data(
            parquet_root=options_parquet_root,
            trade_date=trade_date,
            expiry=market["weekly_expiry"],
            strike=leg["meta"]["strike"],
            option_type=leg["meta"]["type"]
        )

        # ✅ SAFE: Get EOD candle with fallback
        candle, actual_exit_time, warning_msg = _safe_get_candle(opt_df, eod_time, fallback="last")
        
        if candle is None:
            warnings.append({
                "DATE": trade_date,
                "INDEX": index,
                "EXPIRY": market["weekly_expiry"].strftime("%Y-%m-%d"),
                "ACTION": "EOD_EXIT",
                "STRIKE": leg["meta"]["strike"],
                "TYPE": leg["meta"]["type"],
                "REQUESTED_TIME": str(eod_time),
                "ACTUAL_TIME": "N/A",
                "WARNING": "No EOD candle found - leg skipped",
                "EXIT_REASON": "EOD_SKIPPED"
            })
            continue
        
        if warning_msg:
            warnings.append({
                "DATE": trade_date,
                "INDEX": index,
                "EXPIRY": market["weekly_expiry"].strftime("%Y-%m-%d"),
                "ACTION": "EOD_EXIT",
                "STRIKE": leg["meta"]["strike"],
                "TYPE": leg["meta"]["type"],
                "REQUESTED_TIME": str(eod_time),
                "ACTUAL_TIME": str(actual_exit_time),
                "WARNING": warning_msg,
                "EXIT_REASON": "EOD"
            })

        exit_price = candle["Close"]  # EOD exit on CLOSE
        pnl = (exit_price - leg["entry_price"]) * -1


        if minute_pnl_tracker is not None:
            minute_pnl_tracker.add_realized(pnl)
        # Extract metadata
        meta = leg["meta"]
        sl_before_round = meta.get("sl_before_round", None)
        sl_index = meta.get("sl_index", None)
        volatility = meta.get("volatility", None)
        entry_index_price = meta.get("entry_index_price", None)
        upper_range = meta.get("upper", None)
        lower_range = meta.get("lower", None)
        range_used = meta.get("R", None)
        ref_price = meta.get("ref_price", None)

        # Get EOD index price (close)
        eod_index_row = index_df.loc[index_df.index.time == eod_time]
        if not eod_index_row.empty:
            eod_index_price = eod_index_row.iloc[0]["Close"]
        else:
            # Fallback: get last available index price
            eod_index_price = index_df.iloc[-1]["Close"] if not index_df.empty else None

        trades.append({
            "DATE": trade_date,
            "INDEX": index,
            "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
            "DAY": market["day"],
            
            # Entry info
            "ENTRY_TIME": leg["entry_time"].strftime("%H:%M") if hasattr(leg["entry_time"], 'strftime') else str(leg["entry_time"])[:5],
            "INDEX_ENTRY_PRICE": entry_index_price if entry_index_price is not None else ref_price,
            "ENTRY_PRICE": leg["entry_price"],
            
            # Exit info
            "EXIT_TIME": actual_exit_time.strftime("%H:%M") if hasattr(actual_exit_time, 'strftime') else str(actual_exit_time)[:5],
            "INDEX_EXIT_PRICE": eod_index_price,
            "EXIT_PRICE": exit_price,
            "EXIT_REASON": "EOD",
            
            # Strike and type
            "STRIKE": meta["strike"],
            "TYPE": meta["type"],
            
            # Strategy-specific fields (volatility strategy)
            "SL_INDEX": sl_index,
            "SL_BEFORE_ROUND": sl_before_round,
            "VOLATILITY": volatility,
            
            # Strategy-specific fields (inventory strategy)
            "UPPER_RANGE": upper_range,
            "LOWER_RANGE": lower_range,
            "RANGE_USED": range_used,
            
            # PnL
            "QTY": -1,
            "PNL": pnl,
        })

    # -------------------------------------------------
    # SORT TRADES: DATE → ENTRY_TIME → TYPE
    # -------------------------------------------------
    trades.sort(
        key=lambda x: (
            x["DATE"],
            x["ENTRY_TIME"],
            x["TYPE"]
        )
    )

    return trades, warnings