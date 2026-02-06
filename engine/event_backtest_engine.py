from data.index_reader import read_index_data
from data.market_calendar import get_market_context
from data.options_reader import load_option_data


def run_event_backtest(
    trade_date: str,
    index: str,
    index_parquet_map: dict,
    calendar_csv: str,
    options_parquet_root: str,
    strategy
):
    """
    ORIGINAL event backtest engine (V1) - for backward compatibility.
    
    Uses OPEN-based logic:
    - Breach detection on index OPEN
    - Exits on option OPEN
    - Re-entries on same candle's OPEN
    - EOD exits on option OPEN
    
    Used by: DynamicATMInventory and other existing strategies
    """
    trades = []

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

    open_legs = {}

    # -------------------------------------------------
    # INTRADAY LOOP (STRICTLY < EXIT_TIME)
    # -------------------------------------------------
    for ts, row in index_df.iterrows():
        candle_time = ts.time()

        if candle_time >= strategy.EXIT_TIME:
            break

        # 🔑 INDEX PRICE = OPEN (original behavior)
        index_price = row["Open"]

        actions = strategy.on_minute(ts, index_price)

        for a in actions:

            # ================= ENTRY =================
            if a["action"] == "ENTER":
                opt_df = load_option_data(
                    parquet_root=options_parquet_root,
                    trade_date=trade_date,
                    expiry=market["weekly_expiry"],
                    strike=a["strike"],
                    option_type=a["type"]
                )

                candle = opt_df.loc[
                    opt_df.index.time == candle_time
                ].iloc[0]

                open_legs[a["leg_id"]] = {
                    "meta": a,
                    "entry_price": candle["Open"],
                    "entry_time": candle_time
                }

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

                candle = opt_df.loc[
                    opt_df.index.time == candle_time
                ].iloc[0]

                exit_price = candle["Open"]  # Original: exit on OPEN
               
                pnl = (exit_price - leg["entry_price"]) * -1

                # 🔍 RANGE MASKING FOR TRADESHEET (original format)
                upper_range = leg["meta"].get("upper") if leg["meta"]["type"] == "CE" else None
                lower_range = leg["meta"].get("lower") if leg["meta"]["type"] == "PE" else None

                trades.append({
                    "DATE": trade_date,
                    "INDEX": index,
                    "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "DAY": market["day"],
                    "RANGE_USED": leg["meta"].get("R"),
                    "INDEX_PRICE": leg["meta"].get("ref_price", leg["meta"].get("entry_index_price")),
                    "UPPER_RANGE": upper_range,
                    "LOWER_RANGE": lower_range,
                    "ENTRY_TIME": leg["entry_time"].strftime("%H:%M"),
                    "EXIT_TIME": candle_time.strftime("%H:%M"),
                    "INDEX_ENTRY": leg["meta"].get("ref_price", leg["meta"].get("entry_index_price")),
                    "INDEX_EXIT": index_price,
                    "STRIKE": leg["meta"]["strike"],
                    "TYPE": leg["meta"]["type"],
                    "ENTRY_PRICE": leg["entry_price"],
                    "EXIT_PRICE": exit_price,
                    "QTY": -1,
                    "PNL": pnl,
                    "EXIT_REASON": a["reason"],
                })

    # -------------------------------------------------
    # 🔒 EOD EXIT — EXACTLY AT EXIT_TIME (OPEN PRICE - original)
    # -------------------------------------------------
    eod_time = strategy.EXIT_TIME

    for leg_id, leg in open_legs.items():
        opt_df = load_option_data(
            parquet_root=options_parquet_root,
            trade_date=trade_date,
            expiry=market["weekly_expiry"],
            strike=leg["meta"]["strike"],
            option_type=leg["meta"]["type"]
        )

        candle = opt_df.loc[
            opt_df.index.time == eod_time
        ].iloc[0]

        exit_price = candle["Open"]  # Original: EOD exit on OPEN
        pnl = (exit_price - leg["entry_price"]) * -1

        # 🔍 RANGE MASKING FOR TRADESHEET
        upper_range = leg["meta"].get("upper") if leg["meta"]["type"] == "CE" else None
        lower_range = leg["meta"].get("lower") if leg["meta"]["type"] == "PE" else None

        trades.append({
            "DATE": trade_date,
            "INDEX": index,
            "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
            "DAY": market["day"],
            "RANGE_USED": leg["meta"].get("R"),
            "REF_PRICE": leg["meta"].get("ref_price", leg["meta"].get("entry_index_price")),
            "UPPER_RANGE": upper_range,
            "LOWER_RANGE": lower_range,
            "ENTRY_TIME": leg["entry_time"].strftime("%H:%M"),
            "EXIT_TIME": eod_time.strftime("%H:%M"),
            "INDEX_ENTRY": leg["meta"].get("ref_price", leg["meta"].get("entry_index_price")),
            "INDEX_EXIT": None,
            "STRIKE": leg["meta"]["strike"],
            "TYPE": leg["meta"]["type"],
            "ENTRY_PRICE": leg["entry_price"],
            "EXIT_PRICE": exit_price,
            "QTY": -1,
            "PNL": pnl,
            "EXIT_REASON": "EOD",
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

    return trades


# =================================================
# V2 ENGINE - NEW CLOSE-BASED LOGIC
# =================================================

def run_event_backtest_v2(
    trade_date: str,
    index: str,
    index_parquet_map: dict,
    calendar_csv: str,
    options_parquet_root: str,
    strategy
):
    """
    Version 2 of event backtest engine with improved entry/exit logic.
    
    KEY DIFFERENCES FROM V1:
    - Uses CLOSE for breach detection (vs OPEN)
    - Exits on CLOSE when SL hit (vs OPEN)
    - Re-entries on NEXT candle's OPEN (vs same candle)
    - EOD exits on CLOSE (vs OPEN)
    
    Use this for strategies that need precise CLOSE-based exits.
    Use run_event_backtest() for backward compatibility.
    """
    trades = []

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

            candle = opt_df.loc[
                opt_df.index.time == candle_time
            ].iloc[0]

            open_legs[pending["leg_id"]] = {
                "meta": pending,
                "entry_price": candle["Open"],  # ENTER ON OPEN
                "entry_time": candle_time
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

                    candle = opt_df.loc[
                        opt_df.index.time == candle_time
                    ].iloc[0]

                    open_legs[a["leg_id"]] = {
                        "meta": a,
                        "entry_price": candle["Open"],
                        "entry_time": candle_time
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

                candle = opt_df.loc[
                    opt_df.index.time == candle_time
                ].iloc[0]

                # EXIT ON CLOSE (when breach detected)
                exit_price = candle["Close"]
               
                pnl = (exit_price - leg["entry_price"]) * -1
                
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
                    "ENTRY_TIME": leg["entry_time"].strftime("%H:%M"),
                    "INDEX_ENTRY_PRICE": entry_index_price if entry_index_price is not None else ref_price,
                    "ENTRY_PRICE": leg["entry_price"],
                    
                    # Exit info
                    "EXIT_TIME": candle_time.strftime("%H:%M"),
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

    # -------------------------------------------------
    # 🔒 EOD EXIT — EXACTLY AT EXIT_TIME (CLOSE PRICE)
    # -------------------------------------------------
    eod_time = strategy.EXIT_TIME

    for leg_id, leg in open_legs.items():
        opt_df = load_option_data(
            parquet_root=options_parquet_root,
            trade_date=trade_date,
            expiry=market["weekly_expiry"],
            strike=leg["meta"]["strike"],
            option_type=leg["meta"]["type"]
        )

        candle = opt_df.loc[
            opt_df.index.time == eod_time
        ].iloc[0]

        exit_price = candle["Close"]  # EOD exit on CLOSE
        pnl = (exit_price - leg["entry_price"]) * -1

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
        eod_index_price = eod_index_row.iloc[0]["Close"] if not eod_index_row.empty else None

        trades.append({
            "DATE": trade_date,
            "INDEX": index,
            "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
            "DAY": market["day"],
            
            # Entry info
            "ENTRY_TIME": leg["entry_time"].strftime("%H:%M"),
            "INDEX_ENTRY_PRICE": entry_index_price if entry_index_price is not None else ref_price,
            "ENTRY_PRICE": leg["entry_price"],
            
            # Exit info
            "EXIT_TIME": eod_time.strftime("%H:%M"),
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

    return trades