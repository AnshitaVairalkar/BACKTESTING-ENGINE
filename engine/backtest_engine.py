from data.index_reader import read_index_data
from data.market_calendar import get_market_context
from data.options_reader import load_option_data
from engine.execution import execute_option_leg
from strategy.base_strategy import BaseStrategy


def run_single_day_backtest(
    trade_date: str,
    index_parquet: str,
    calendar_csv: str,
    options_parquet_root: str,
    strategy: BaseStrategy
):
    """
    Run backtest for a single trading day.
    
    Args:
        trade_date: Trading date (YYYY-MM-DD)
        index_parquet: Path to index parquet file
        calendar_csv: Path to market calendar CSV
        options_parquet_root: Root path to options parquet data
        strategy: Strategy instance
        
    Returns:
        Tuple of (trades_list, errors_list)
    """
    trades = []
    errors = []
    
    try:
        # Load market data
        index_df = read_index_data(index_parquet, trade_date)
        market = get_market_context(calendar_csv, trade_date)

        # Get spot price at entry time
        entry_time_str = strategy.ENTRY_TIME.strftime("%H:%M")
        spot_df = index_df.between_time(entry_time_str, entry_time_str)
        
        if spot_df.empty:
            raise ValueError(f"Spot {entry_time_str} candle missing for {trade_date}")

        spot_price = spot_df.iloc[0]["Close"]

        # Calculate strikes
        strikes = strategy.get_strikes(spot_price)

        # Execute each leg
        for leg_id, strike in strikes.items():
            try:
                # Determine option type from leg_id
                if "CE" in leg_id:
                    option_type = "CE"
                elif "PE" in leg_id:
                    option_type = "PE"
                else:
                    raise ValueError(f"Cannot determine option type from leg_id: {leg_id}")
                
                # Load option data
                opt_df = load_option_data(
                    parquet_root=options_parquet_root,
                    trade_date=trade_date,
                    expiry=market["weekly_expiry"],
                    strike=strike,
                    option_type=option_type
                )

                # Get quantity for this leg
                qty = strategy.get_leg_qty(leg_id)

                # Execute leg
                exec_result = execute_option_leg(
                    df=opt_df,
                    intended_entry_time=strategy.ENTRY_TIME,
                    exit_time=strategy.EXIT_TIME,
                    sl_pct=strategy.SL_PCT,
                    qty=qty
                )

                # Build trade record
                trade = {
                    "Date": trade_date,
                    "Strategy": strategy.get_strategy_name(),
                    "LegID": leg_id,
                    "OptionType": option_type,
                    "Strike": strike,
                    "Expiry": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "DTE": market["dte_weekly"],
                    "SpotPrice": spot_price,
                    
                    "IntendedEntryTime": exec_result["intended_entry_time"],
                    "EntryTime": exec_result["entry_time"],
                    "EntryPrice": exec_result["entry_price"],
                    "EntryDelayed": exec_result["entry_delayed"],
                    "EntryDelayMinutes": exec_result["entry_delay_minutes"],
                    
                    "ExitTime": exec_result["exit_time"],
                    "ExitPrice": exec_result["exit_price"],
                    "ExitReason": exec_result["exit_reason"],
                    "SLPrice": exec_result["sl_price"],
                    
                    "Qty": exec_result["qty"],
                    "PnL": exec_result["pnl"],
                }
                
                trades.append(trade)

            except Exception as e:
                # ✅ FIX: Capture leg-level errors with full details
                error_record = {
                    "Date": trade_date,
                    "Strategy": strategy.get_strategy_name(),
                    "LegID": leg_id,
                    "OptionType": option_type if 'option_type' in locals() else "UNKNOWN",
                    "Strike": strike,
                    "Expiry": market["weekly_expiry"].strftime("%Y-%m-%d"),
                    "Error": str(e)
                }
                errors.append(error_record)
                print(f"  ⚠️  Error processing {leg_id} {option_type if 'option_type' in locals() else ''} {strike}: {e}")
                continue
    
    except Exception as e:
        # Day-level error (index data, market calendar, etc.)
        error_record = {
            "Date": trade_date,
            "Strategy": strategy.get_strategy_name(),
            "LegID": "ALL",
            "OptionType": "N/A",
            "Strike": 0,
            "Expiry": "N/A",
            "Error": str(e)
        }
        errors.append(error_record)
        print(f"  ✗ Day-level error: {e}")

    return trades, errors


def run_multi_day_backtest(
    dates: list,
    index_parquet: str,
    calendar_csv: str,
    options_parquet_root: str,
    strategy: BaseStrategy,
    verbose: bool = True
):
    """
    Run backtest for multiple trading days.
    
    Args:
        dates: List of trading dates (YYYY-MM-DD)
        index_parquet: Path to index parquet file
        calendar_csv: Path to market calendar CSV
        options_parquet_root: Root path to options parquet data
        strategy: Strategy instance
        verbose: Print progress
        
    Returns:
        Tuple of (all_trades, all_errors)
    """
    all_trades = []
    all_errors = []
    
    total = len(dates)
    
    if verbose:
        print(f"\n🎯 Running strategy: {strategy.get_strategy_name()}")
        print(f"   Entry: {strategy.ENTRY_TIME.strftime('%H:%M')}")
        print(f"   Exit: {strategy.EXIT_TIME.strftime('%H:%M')}")
        print(f"   Stop Loss: {strategy.SL_PCT * 100}%\n")
    
    for i, trade_date in enumerate(dates, 1):
        if verbose:
            print(f"[{i}/{total}] {trade_date}", end=" ")
        
        # ✅ FIX: Collect both trades AND errors from each day
        trades, errors = run_single_day_backtest(
            trade_date=trade_date,
            index_parquet=index_parquet,
            calendar_csv=calendar_csv,
            options_parquet_root=options_parquet_root,
            strategy=strategy
        )
        
        all_trades.extend(trades)
        all_errors.extend(errors)
        
        if verbose:
            if trades:
                total_pnl = sum(t["PnL"] for t in trades)
                print(f"✓ Trades: {len(trades)} | PnL: {total_pnl:+.2f}")
            else:
                print(f"✓ Trades: 0 | PnL: +0.00")
    
    return all_trades, all_errors