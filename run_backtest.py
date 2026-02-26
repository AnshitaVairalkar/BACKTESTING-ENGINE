import pandas as pd
from pathlib import Path
from datetime import datetime
from data.index_reader import IndexDataStore
from data.options_reader import clear_cache
from engine.backtest_engine import run_multi_day_backtest
from engine.event_backtest_engine import run_event_backtest_v2
from analytics.minute_pnl_tracker import MinutePnLTracker



# =================================================
#  CONFIGURATION
# =================================================

#  INDEX SELECTION
INDEX = "NIFTY"   # "NIFTY" or "SENSEX"

#  STRATEGY SELECTION
from strategy.itm_straddle import ITMStraddle
from strategy.dynamic_atm_inventory import DynamicATMInventory
from strategy.volatility_strangles import VolatilityStrangles
from strategy.volatility_straddles import VolatilityStraddles
from strategy.dynamic_atm_latestlevelcheck import DynamicATMInventoryLatestLevelCheck

# Initialize VolatilityStraddles with path to volatility CSV
ROOT = Path(__file__).resolve().parent
VOLATILITY_CSV = ROOT / "data" / "nifty_daily_volatility.csv"  # Adjust path as needed

strategy = VolatilityStrangles(volatility_csv_path=str(VOLATILITY_CSV))
# strategy= VolatilityStraddles(volatility_csv_path=str(VOLATILITY_CSV))
# strategy = ITMStraddle()
# strategy = DynamicATMInventory()
# strategy = DynamicATMInventoryLatestLevelCheck()

# 🔹 DATE RANGE
START_DATE = "2022-01-01"
END_DATE   = "2025-12-31"

# =================================================
# PATHS
# =================================================

INDEX_PARQUET_MAP = {
    "SENSEX": ROOT / "../Index Data/SENSEX/SENSEX_IDX.parquet",
    "NIFTY":  ROOT / "../Index Data/NIFTY/NIFTY_IDX.parquet",
}

CALENDAR_CSV_MAP = {
    "SENSEX": ROOT / "../ExpiryDates/SENSEX_market_dates.csv",
    "NIFTY":  ROOT / "../ExpiryDates/NIFTY_market_dates.csv",
}

OPTIONS_PARQUET_MAP = {
    "SENSEX": ROOT / "../Options_Parquet/SENSEX",
    "NIFTY":  ROOT / "../Options_Parquet/NIFTY",
}

OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 50

# =================================================
# MAIN
# =================================================

def main():
    print("=" * 70)
    print("OPTIONS BACKTEST ENGINE")
    print("=" * 70)

    start_time = datetime.now()
    strategy_name = strategy.get_strategy_name()

    print(f"\n📊 Strategy: {strategy_name}")
    print(f"📈 Index: {INDEX}")
    print(f"Entry Time: {strategy.ENTRY_TIME.strftime('%H:%M')}")
    print(f"Exit Time: {strategy.EXIT_TIME.strftime('%H:%M')}")

    # -------------------------------------------------
    # LOAD INDEX DATES
    # -------------------------------------------------
    IndexDataStore.load(str(INDEX_PARQUET_MAP[INDEX]))
    index_dates = set(
        IndexDataStore._df.index.normalize().strftime("%Y-%m-%d")
    )

    calendar_df = pd.read_csv(CALENDAR_CSV_MAP[INDEX], parse_dates=["Date"])
    calendar_df["DateStr"] = calendar_df["Date"].dt.strftime("%Y-%m-%d")

    trading_dates = sorted(set(calendar_df["DateStr"]) & index_dates)

    if START_DATE:
        trading_dates = [d for d in trading_dates if d >= START_DATE]
    if END_DATE:
        trading_dates = [d for d in trading_dates if d <= END_DATE]

    # -------------------------------------------------
    # OUTPUT FILES
    # -------------------------------------------------
    date_suffix = f"{START_DATE}_{END_DATE}".replace("-", "")
    trades_file = OUTPUT_DIR / f"{INDEX.lower()}_{strategy_name.lower()}_{date_suffix}.csv"
    issues_file = OUTPUT_DIR / f"{INDEX.lower()}_{strategy_name.lower()}_issues_{date_suffix}.csv"  # ✅ Combined errors + warnings

    all_trades = []
    all_issues = []  # ✅ Combined list for errors and warnings

    # =================================================
    # ENGINE MODE
    # =================================================
    is_event_strategy = hasattr(strategy, "on_minute")

    print(
        "\n🧠 Engine Mode:",
        "EVENT-DRIVEN" if is_event_strategy else "LEG-BASED"
    )

    # 1-min PnL tracker filename: e.g. nifty_volatilitystrangles_20210601_20251231
    pnl_filename = f"{INDEX.lower()}_{strategy_name.lower()}_{START_DATE.replace('-', '')}_{END_DATE.replace('-', '')}"
    minute_pnl_tracker = MinutePnLTracker(pnl_filename, OUTPUT_DIR) if is_event_strategy else None

    # =================================================
    # RUN BACKTEST
    # =================================================
    for i, trade_date in enumerate(trading_dates, 1):
        print(f"[{i}/{len(trading_dates)}] {trade_date}", end="")

        try:
            if is_event_strategy:
                # Use V2 engine for VolatilityStrangles (CLOSE-based logic)
                # Use V1 engine for other strategies (OPEN-based logic for backward compatibility)
                trades, warnings = run_event_backtest_v2(
                        trade_date=trade_date,
                        index=INDEX,
                        index_parquet_map=INDEX_PARQUET_MAP,
                        calendar_csv=str(CALENDAR_CSV_MAP[INDEX]),
                        options_parquet_root=str(OPTIONS_PARQUET_MAP[INDEX]),
                        strategy=strategy,
                        minute_pnl_tracker=minute_pnl_tracker
                    )
                
                all_trades.extend(trades)
                
                # ✅ Add warnings to issues with TYPE = WARNING
                if warnings:
                    for w in warnings:
                        all_issues.append({
                            "DATE": w.get("DATE"),
                            "INDEX": w.get("INDEX"),
                            "STRATEGY": strategy_name,
                            "TYPE": "WARNING",
                            "ACTION": w.get("ACTION"),
                            "EXPIRY": w.get("EXPIRY"),
                            "STRIKE": w.get("STRIKE"),
                            "OPT_TYPE": w.get("TYPE"),
                            "REQUESTED_TIME": w.get("REQUESTED_TIME"),
                            "ACTUAL_TIME": w.get("ACTUAL_TIME"),
                            "MESSAGE": w.get("WARNING"),
                        })
                    print(f" ⚠️ {len(warnings)} warning(s)")
                else:
                    print(" ✓")

            else:
                trades, _ = run_multi_day_backtest(
                    dates=[trade_date],
                    index_parquet=str(INDEX_PARQUET_MAP[INDEX]),
                    calendar_csv=str(CALENDAR_CSV_MAP[INDEX]),
                    options_parquet_root=str(OPTIONS_PARQUET_MAP[INDEX]),
                    strategy=strategy,
                    verbose=False
                )
                all_trades.extend(trades)
                print(" ✓")

        except Exception as e:
            # 🔴 LOG ERROR with TYPE = ERROR
            all_issues.append({
                "DATE": trade_date,
                "INDEX": INDEX,
                "STRATEGY": strategy_name,
                "TYPE": "ERROR",
                "ACTION": None,
                "EXPIRY": None,
                "STRIKE": None,
                "OPT_TYPE": None,
                "REQUESTED_TIME": None,
                "ACTUAL_TIME": None,
                "MESSAGE": str(e),
            })
            print(f" ❌ Error: {e}")
            continue

        if i % BATCH_SIZE == 0:
            clear_cache()

    # -------------------------------------------------
    # SAVE RESULTS
    # -------------------------------------------------
    if minute_pnl_tracker is not None:
        minute_pnl_tracker.save()

    if all_trades:
        pd.DataFrame(all_trades).to_csv(trades_file, index=False)
        print(f"\n✅ Trades saved to {trades_file}")
        print(f"   Total trades: {len(all_trades)}")

    # ✅ Save combined issues file
    if all_issues:
        issues_df = pd.DataFrame(all_issues)
        issues_df.to_csv(issues_file, index=False)
        print(f"\n📋 Issues logged to {issues_file}")
        
        # Count by type
        error_count = len(issues_df[issues_df["TYPE"] == "ERROR"])
        warning_count = len(issues_df[issues_df["TYPE"] == "WARNING"])
        
        print(f"   ❌ Errors: {error_count}")
        print(f"   ⚠️ Warnings: {warning_count}")
        
        # Summary by action type for warnings
        if warning_count > 0:
            print("\n   Warning Summary by Action:")
            warning_summary = issues_df[issues_df["TYPE"] == "WARNING"].groupby("ACTION").size()
            for action, count in warning_summary.items():
                print(f"      {action}: {count}")

    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n⏱ Runtime: {duration:.2f} seconds")
    print("=" * 70)


if __name__ == "__main__":
    main()