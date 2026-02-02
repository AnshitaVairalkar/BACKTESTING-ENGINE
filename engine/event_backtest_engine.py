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

        # 🔑 INDEX PRICE = OPEN
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

                exit_price = candle["Open"]
               
                pnl= (exit_price - leg["entry_price"]) * -1
                

                # 🔍 RANGE MASKING FOR TRADESHEET
                upper_range = leg["meta"]["upper"] if leg["meta"]["type"] == "CE" else None
                lower_range = leg["meta"]["lower"] if leg["meta"]["type"] == "PE" else None

                trades.append({
                    "DATE": trade_date,
                    "INDEX": index,
                    "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "DAY": market["day"],
                    "RANGE_USED": leg["meta"]["R"],
                    "INDEX_PRICE": leg["meta"]["ref_price"],
                    "UPPER_RANGE": upper_range,
                    "LOWER_RANGE": lower_range,
                    "ENTRY_TIME": leg["entry_time"].strftime("%H:%M"),
                    "EXIT_TIME": candle_time.strftime("%H:%M"),
                    "INDEX_ENTRY": leg["meta"]["ref_price"],
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
    # 🔒 EOD EXIT — EXACTLY AT EXIT_TIME (OPEN PRICE)
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

        exit_price = candle["Open"]
        pnl =(exit_price - leg["entry_price"]) * -1

        # 🔍 RANGE MASKING FOR TRADESHEET
        upper_range = leg["meta"]["upper"] if leg["meta"]["type"] == "CE" else None
        lower_range = leg["meta"]["lower"] if leg["meta"]["type"] == "PE" else None

        trades.append({
            "DATE": trade_date,
            "INDEX": index,
            "EXPIRYDATE": market["weekly_expiry"].strftime("%Y-%m-%d"),
            "DAY": market["day"],
            "RANGE_USED": leg["meta"]["R"],
            "REF_PRICE": leg["meta"]["ref_price"],
            "UPPER_RANGE": upper_range,
            "LOWER_RANGE": lower_range,
            "ENTRY_TIME": leg["entry_time"].strftime("%H:%M"),
            "EXIT_TIME": eod_time.strftime("%H:%M"),
            "INDEX_ENTRY": leg["meta"]["ref_price"],
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
