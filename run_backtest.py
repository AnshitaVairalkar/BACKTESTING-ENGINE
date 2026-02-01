import pandas as pd
from pathlib import Path
from datetime import datetime

from engine.backtest_engine import run_multi_day_backtest
from data.index_reader import IndexDataStore
from data.options_reader import clear_cache, get_cache_stats

# ═══════════════════════════════════════════════════════════════
# 🎯 CONFIGURATION - EDIT THESE SETTINGS
# ═══════════════════════════════════════════════════════════════

# 1️⃣ SELECT YOUR STRATEGY (uncomment one)
from strategy.itm_straddle import ITMStraddle
# from strategy.atm_straddle import ATMStraddle
# from strategy.otm_straddle import OTMStraddle
# from strategy.iron_condor import IronCondor

strategy = ITMStraddle()
# strategy = ATMStraddle()
# strategy = OTMStraddle()
# strategy = IronCondor()

# 2️⃣ DATE RANGE (set to None for all available dates)
START_DATE = "2024-12-01"  # Format: YYYY-MM-DD or None
END_DATE = "2025-01-31"    # Format: YYYY-MM-DD or None

# Examples:
# START_DATE = "2024-01-01"  # Start from Jan 2024
# END_DATE = "2024-12-31"    # End at Dec 2024
# START_DATE = None          # Start from first available date
# END_DATE = None            # Run till last available date

# ═══════════════════════════════════════════════════════════════

# -------------------------------------------------
# PATHS
# -------------------------------------------------
ROOT = Path(__file__).resolve().parent

INDEX_PARQUET = ROOT / "../Index Data/SENSEX/SENSEX_IDX.parquet"
CALENDAR_CSV = ROOT / "../ExpiryDates/SENSEX_market_dates.csv"
OPTIONS_PARQUET = ROOT / "../Options_Parquet/SENSEX"

OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# -------------------------------------------------
# CONFIGURATION
# -------------------------------------------------
BATCH_SIZE = 50  # Clear cache every N days


def main():
    print("=" * 70)
    print("OPTIONS BACKTEST ENGINE")
    print("=" * 70)
    
    start_time = datetime.now()
    
    # Show strategy info
    strategy_name = strategy.get_strategy_name()
    print(f"\n📊 Strategy: {strategy_name}")
    print(f"   Entry Time: {strategy.ENTRY_TIME.strftime('%H:%M')}")
    print(f"   Exit Time: {strategy.EXIT_TIME.strftime('%H:%M')}")
    print(f"   Stop Loss: {strategy.SL_PCT * 100}%")
    print(f"   Strike Gap: {strategy.STRIKE_GAP}")
    
    print(f"\n📅 Date Range:")
    if START_DATE and END_DATE:
        print(f"   From: {START_DATE}")
        print(f"   To: {END_DATE}")
    elif START_DATE:
        print(f"   From: {START_DATE} (to last available date)")
    elif END_DATE:
        print(f"   From: First available date to {END_DATE}")
    else:
        print(f"   All available dates")
    
    # Setup output files
    strategy_slug = strategy_name.lower()
    
    # Add date range to filename
    date_suffix = ""
    if START_DATE and END_DATE:
        start_year = START_DATE[:4]
        end_year = END_DATE[:4]
        if start_year == end_year:
            date_suffix = f"_{start_year}"
        else:
            date_suffix = f"_{start_year}_{end_year}"
    elif START_DATE:
        date_suffix = f"_from_{START_DATE[:4]}"
    elif END_DATE:
        date_suffix = f"_to_{END_DATE[:4]}"
    else:
        date_suffix = "_all"
    
    OUTPUT_FILE = OUTPUT_DIR / f"sensex_{strategy_slug}{date_suffix}.csv"
    ERROR_FILE = OUTPUT_DIR / f"errors_{strategy_slug}{date_suffix}.csv"
    
    # -------------------------------------------------
    # LOAD INDEX DATES
    # -------------------------------------------------
    print("\n📊 Loading index data...")
    IndexDataStore.load(str(INDEX_PARQUET))
    index_dates = set(
        IndexDataStore._df.index.normalize().strftime("%Y-%m-%d")
    )
    print(f"   Index dates available: {len(index_dates)}")

    # -------------------------------------------------
    # LOAD CALENDAR & INTERSECT
    # -------------------------------------------------
    print("\n📅 Loading market calendar...")
    calendar_df = pd.read_csv(CALENDAR_CSV, parse_dates=["Date"])
    calendar_df["DateStr"] = calendar_df["Date"].dt.strftime("%Y-%m-%d")

    trading_dates = sorted(set(calendar_df["DateStr"]) & index_dates)
    print(f"   Total trading days: {len(trading_dates)}")
    
    # -------------------------------------------------
    # APPLY DATE RANGE FILTER
    # -------------------------------------------------
    if START_DATE or END_DATE:
        original_count = len(trading_dates)
        
        if START_DATE:
            trading_dates = [d for d in trading_dates if d >= START_DATE]
            print(f"   Filtered from: {START_DATE}")
        
        if END_DATE:
            trading_dates = [d for d in trading_dates if d <= END_DATE]
            print(f"   Filtered to: {END_DATE}")
        
        filtered_count = len(trading_dates)
        print(f"   Dates after filter: {filtered_count} (removed {original_count - filtered_count})")
    
    if not trading_dates:
        print("\n❌ No trading dates in the specified range!")
        return

    # -------------------------------------------------
    # RUN BACKTEST
    # -------------------------------------------------
    print(f"\n🚀 Starting backtest...")
    print(f"   Batch size: {BATCH_SIZE} days\n")
    
    all_trades = []
    all_errors = []
    
    # Process in batches
    for batch_start in range(0, len(trading_dates), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(trading_dates))
        batch_dates = trading_dates[batch_start:batch_end]
        
        print(f"📦 Batch {batch_start//BATCH_SIZE + 1} "
              f"(Days {batch_start + 1}-{batch_end})")
        
        # Run batch
        trades, errors = run_multi_day_backtest(
            dates=batch_dates,
            index_parquet=str(INDEX_PARQUET),
            calendar_csv=str(CALENDAR_CSV),
            options_parquet_root=str(OPTIONS_PARQUET),
            strategy=strategy,
            verbose=True
        )
        
        all_trades.extend(trades)
        all_errors.extend(errors)
        
        # Cache stats
        cache_stats = get_cache_stats()
        print(f"   Cache: {cache_stats['cached_expiries']} expiries loaded\n")
        
        # Clear cache after batch
        clear_cache()

    # -------------------------------------------------
    # CALCULATE SUMMARY
    # -------------------------------------------------
    print("\n" + "=" * 70)
    print("BACKTEST SUMMARY")
    print("=" * 70)
    
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        
        # Daily PnL
        daily_pnl = trades_df.groupby("Date")["PnL"].sum()
        
        # Stats
        total_pnl = daily_pnl.sum()
        total_days = len(daily_pnl)
        winning_days = (daily_pnl > 0).sum()
        losing_days = (daily_pnl < 0).sum()
        
        print(f"\n📈 Performance:")
        print(f"   Strategy: {strategy_name}")
        print(f"   Total P&L: {total_pnl:+,.2f}")
        print(f"   Trading Days: {total_days}")
        print(f"   Winning Days: {winning_days} ({winning_days/total_days*100:.1f}%)")
        print(f"   Losing Days: {losing_days} ({losing_days/total_days*100:.1f}%)")
        print(f"   Avg Daily P&L: {daily_pnl.mean():+,.2f}")
        print(f"   Max Daily Win: {daily_pnl.max():+,.2f}")
        print(f"   Max Daily Loss: {daily_pnl.min():+,.2f}")
        
        print(f"\n📊 Trade Stats:")
        print(f"   Total Trades: {len(all_trades)}")
        print(f"   SL Hit: {(trades_df['ExitReason'] == 'SL_HIT').sum()}")
        print(f"   Time Exit: {(trades_df['ExitReason'] == 'TIME_EXIT').sum()}")
        
        # Leg breakdown
        if "LegID" in trades_df.columns:
            print(f"\n📊 Leg Breakdown:")
            for leg_id in sorted(trades_df["LegID"].unique()):
                leg_pnl = trades_df[trades_df["LegID"] == leg_id]["PnL"].sum()
                leg_count = len(trades_df[trades_df["LegID"] == leg_id])
                print(f"   {leg_id}: {leg_pnl:+,.2f} ({leg_count} trades)")

    # -------------------------------------------------
    # SAVE OUTPUTS
    # -------------------------------------------------
    print(f"\n💾 Saving results...")
    
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_df.to_csv(OUTPUT_FILE, index=False)
        print(f"   ✅ Trades saved: {OUTPUT_FILE}")
    else:
        print(f"   ⚠️  No trades to save")

    if all_errors:
        pd.DataFrame(all_errors).to_csv(ERROR_FILE, index=False)
        print(f"   ⚠️  Errors logged: {ERROR_FILE}")
        print(f"   Error count: {len(all_errors)}")
    else:
        print(f"   ✅ No errors encountered")

    # -------------------------------------------------
    # TIMING
    # -------------------------------------------------
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n⏱️  Execution Time:")
    print(f"   Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    if trading_dates:
        print(f"   Speed: {len(trading_dates)/duration:.2f} days/second")
    
    print("\n" + "=" * 70)
    print("✅ BACKTEST COMPLETED")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()