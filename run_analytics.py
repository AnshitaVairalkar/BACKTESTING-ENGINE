import pandas as pd
from pathlib import Path

# -------------------------------------------------
# PATHS
# -------------------------------------------------
ROOT = Path(__file__).resolve().parent

TRADES_FILE = ROOT / "output/sensex_itm_straddle_2023_2025.csv"
OUTPUT_FILE = ROOT / "output/strategy_summary.csv"
STRATEGY_NAME = "sensex_itm_straddle_2023_2025"


def calculate_analytics(trades_df: pd.DataFrame) -> dict:
    """
    Calculate comprehensive strategy analytics.
    
    Args:
        trades_df: DataFrame with trade data including PnL
        
    Returns:
        Dictionary with analytics metrics
    """
    # Daily aggregation
    daily_pnl = trades_df.groupby("Date")["PnL"].sum()
    
    # Basic stats
    total_pnl = daily_pnl.sum()
    total_days = len(daily_pnl)
    avg_daily_pnl = daily_pnl.mean()
    
    # Win/Loss analysis
    winning_days = daily_pnl[daily_pnl > 0]
    losing_days = daily_pnl[daily_pnl < 0]
    
    win_count = len(winning_days)
    loss_count = len(losing_days)
    win_ratio = win_count / total_days if total_days > 0 else 0
    loss_ratio = loss_count / total_days if total_days > 0 else 0
    
    avg_win_pnl = winning_days.mean() if len(winning_days) > 0 else 0
    avg_loss_pnl = losing_days.mean() if len(losing_days) > 0 else 0
    
    max_win = daily_pnl.max()
    max_loss = daily_pnl.min()
    
    # Drawdown calculation
    cumulative_pnl = daily_pnl.cumsum()
    running_max = cumulative_pnl.expanding().max()
    drawdown = cumulative_pnl - running_max
    max_drawdown = drawdown.min()
    
    # Time to recovery (days from max drawdown to recovery)
    if max_drawdown < 0:
        dd_idx = drawdown.idxmin()
        recovery_series = cumulative_pnl[dd_idx:]
        dd_value = cumulative_pnl[dd_idx]
        recovery_idx = recovery_series[recovery_series >= running_max[dd_idx]].first_valid_index()
        
        if recovery_idx:
            time_to_recovery = (pd.to_datetime(recovery_idx) - pd.to_datetime(dd_idx)).days
        else:
            time_to_recovery = -1  # Not yet recovered
    else:
        time_to_recovery = 0
    
    # Sortino Ratio (uses downside deviation)
    target_return = 0
    downside_returns = daily_pnl[daily_pnl < target_return]
    downside_std = downside_returns.std() if len(downside_returns) > 0 else 0
    sortino_ratio = avg_daily_pnl / downside_std if downside_std > 0 else 0
    
    # Sharpe Ratio
    daily_std = daily_pnl.std()
    sharpe_ratio = avg_daily_pnl / daily_std if daily_std > 0 else 0
    
    # Trade-level stats
    total_trades = len(trades_df)
    sl_hits = (trades_df["ExitReason"] == "SL_HIT").sum()
    time_exits = (trades_df["ExitReason"] == "TIME_EXIT").sum()
    
    # Leg-level stats
    ce_trades = trades_df[trades_df["OptionType"] == "CE"]
    pe_trades = trades_df[trades_df["OptionType"] == "PE"]
    
    return {
        # Overall Performance
        "TotalPnL": total_pnl,
        "AvgDailyPnL": avg_daily_pnl,
        "MaxDailyWin": max_win,
        "MaxDailyLoss": max_loss,
        "MaxDrawdown": max_drawdown,
        "TimeToRecoveryDays": time_to_recovery,
        
        # Win/Loss Stats
        "TotalDays": total_days,
        "WinningDays": win_count,
        "LosingDays": loss_count,
        "WinRatio": win_ratio,
        "LossRatio": loss_ratio,
        "AvgWinPnL": avg_win_pnl,
        "AvgLossPnL": avg_loss_pnl,
        
        # Risk Metrics
        "SortinoRatio": sortino_ratio,
        "SharpeRatio": sharpe_ratio,
        "DailyStdDev": daily_std,
        
        # Trade Stats
        "TotalTrades": total_trades,
        "SL_Hits": sl_hits,
        "TimeExits": time_exits,
        "SL_HitRate": sl_hits / total_trades if total_trades > 0 else 0,
        
        # Leg Performance
        "CE_PnL": ce_trades["PnL"].sum() if len(ce_trades) > 0 else 0,
        "PE_PnL": pe_trades["PnL"].sum() if len(pe_trades) > 0 else 0,
        "CE_Count": len(ce_trades),
        "PE_Count": len(pe_trades),
    }


def main():
    print("\n" + "=" * 60)
    print("STRATEGY ANALYTICS")
    print("=" * 60 + "\n")
    
    # -------------------------------------------------
    # LOAD TRADES
    # -------------------------------------------------
    print(f"📊 Loading trades from: {TRADES_FILE}")
    
    if not TRADES_FILE.exists():
        print(f"❌ Trades file not found: {TRADES_FILE}")
        print(f"   Please run the backtest first (run_backtest.py)\n")
        return
    
    trades_df = pd.read_csv(TRADES_FILE)
    print(f"   ✅ Loaded {len(trades_df)} trades\n")
    
    # -------------------------------------------------
    # CALCULATE ANALYTICS
    # -------------------------------------------------
    print("🔢 Calculating analytics...")
    stats = calculate_analytics(trades_df)
    
    # -------------------------------------------------
    # PREPARE OUTPUT
    # -------------------------------------------------
    row = {
        "Strategy": STRATEGY_NAME,
        "TotalPnL": round(stats["TotalPnL"], 2),
        "AvgDailyPnL": round(stats["AvgDailyPnL"], 2),
        "MaxDailyWin": round(stats["MaxDailyWin"], 2),
        "MaxDailyLoss": round(stats["MaxDailyLoss"], 2),
        "MaxDrawdown": round(stats["MaxDrawdown"], 2),
        "TimeToRecoveryDays": stats["TimeToRecoveryDays"],
        
        "TotalDays": stats["TotalDays"],
        "WinningDays": stats["WinningDays"],
        "LosingDays": stats["LosingDays"],
        "WinRatio": round(stats["WinRatio"], 4),
        "LossRatio": round(stats["LossRatio"], 4),
        "AvgWinPnL": round(stats["AvgWinPnL"], 2),
        "AvgLossPnL": round(stats["AvgLossPnL"], 2),
        
        "SortinoRatio": round(stats["SortinoRatio"], 4),
        "SharpeRatio": round(stats["SharpeRatio"], 4),
        "DailyStdDev": round(stats["DailyStdDev"], 2),
        
        "TotalTrades": stats["TotalTrades"],
        "SL_Hits": stats["SL_Hits"],
        "TimeExits": stats["TimeExits"],
        "SL_HitRate": round(stats["SL_HitRate"], 4),
        
        "CE_PnL": round(stats["CE_PnL"], 2),
        "PE_PnL": round(stats["PE_PnL"], 2),
    }
    
    df = pd.DataFrame([row])
    
    # -------------------------------------------------
    # SAVE OUTPUT
    # -------------------------------------------------
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"   ✅ Analytics saved to: {OUTPUT_FILE}\n")
    
    # -------------------------------------------------
    # DISPLAY SUMMARY
    # -------------------------------------------------
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    print(f"\n📈 Performance:")
    print(f"   Total P&L: {stats['TotalPnL']:+,.2f}")
    print(f"   Avg Daily P&L: {stats['AvgDailyPnL']:+,.2f}")
    print(f"   Max Daily Win: {stats['MaxDailyWin']:+,.2f}")
    print(f"   Max Daily Loss: {stats['MaxDailyLoss']:+,.2f}")
    print(f"   Max Drawdown: {stats['MaxDrawdown']:+,.2f}")
    print(f"   Recovery Time: {stats['TimeToRecoveryDays']} days")
    
    print(f"\n📊 Win/Loss:")
    print(f"   Trading Days: {stats['TotalDays']}")
    print(f"   Winning Days: {stats['WinningDays']} ({stats['WinRatio']*100:.1f}%)")
    print(f"   Losing Days: {stats['LosingDays']} ({stats['LossRatio']*100:.1f}%)")
    print(f"   Avg Win: {stats['AvgWinPnL']:+,.2f}")
    print(f"   Avg Loss: {stats['AvgLossPnL']:+,.2f}")
    
    print(f"\n📉 Risk Metrics:")
    print(f"   Sortino Ratio: {stats['SortinoRatio']:.4f}")
    print(f"   Sharpe Ratio: {stats['SharpeRatio']:.4f}")
    print(f"   Daily Std Dev: {stats['DailyStdDev']:.2f}")
    
    print(f"\n🎯 Trade Stats:")
    print(f"   Total Trades: {stats['TotalTrades']}")
    print(f"   SL Hits: {stats['SL_Hits']} ({stats['SL_HitRate']*100:.1f}%)")
    print(f"   Time Exits: {stats['TimeExits']} ({(1-stats['SL_HitRate'])*100:.1f}%)")
    
    print(f"\n📊 Leg Performance:")
    print(f"   CE P&L: {stats['CE_PnL']:+,.2f} ({stats['CE_Count']} trades)")
    print(f"   PE P&L: {stats['PE_PnL']:+,.2f} ({stats['PE_Count']} trades)")
    
    print("\n" + "=" * 60)
    print("✅ ANALYTICS COMPLETED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()