import pandas as pd
import numpy as np


# ============================================================
# PnL
# ============================================================

def add_pnl_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["PnL"] = df["EntryPrice"] - df["ExitPrice"]
    df["IsWin"] = df["PnL"] > 0
    return df


def daily_pnl(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("Date", as_index=False)["PnL"]
        .sum()
        .sort_values("Date")
    )


# ============================================================
# DRAWDOWN
# ============================================================

def drawdown_stats(daily: pd.DataFrame) -> dict:
    curve = daily.copy()
    curve["CumPnL"] = curve["PnL"].cumsum()
    curve["Peak"] = curve["CumPnL"].cummax()
    curve["Drawdown"] = curve["CumPnL"] - curve["Peak"]

    max_dd = curve["Drawdown"].min()

    dd_idx = curve["Drawdown"].idxmin()
    dd_date = curve.loc[dd_idx, "Date"]

    recovery = curve[
        (curve["Date"] > dd_date) &
        (curve["CumPnL"] >= curve.loc[dd_idx, "Peak"])
    ]

    recovery_days = (
        (recovery.iloc[0]["Date"] - dd_date).days
        if not recovery.empty else None
    )

    return {
        "MaxDrawdown": max_dd,
        "TimeToRecoveryDays": recovery_days,
    }


# ============================================================
# RATIOS
# ============================================================

def sortino_ratio(daily: pd.DataFrame, rf: float = 0.0) -> float:
    returns = daily["PnL"]
    downside = returns[returns < rf]

    if downside.empty:
        return float("inf")

    downside_std = downside.std()
    if downside_std == 0:
        return float("inf")

    return (returns.mean() - rf) / downside_std


# ============================================================
# TRADE STATS
# ============================================================

def trade_statistics(df: pd.DataFrame) -> dict:
    total = len(df)
    wins = df["IsWin"].sum()
    losses = total - wins

    return {
        "TotalTrades": total,
        "WinningTrades": wins,
        "LosingTrades": losses,
        "WinRatio": wins / total if total else 0,
        "LossRatio": losses / total if total else 0,
    }


# ============================================================
# MASTER ANALYTICS
# ============================================================

def analytics_summary(trades_csv: str) -> dict:
    trades = pd.read_csv(trades_csv, parse_dates=["Date"])
    trades = add_pnl_columns(trades)

    daily = daily_pnl(trades)

    summary = {
        "TotalPnL": trades["PnL"].sum(),
        "AvgDailyPnL": daily["PnL"].mean(),
        "AvgWinPnL": trades.loc[trades["PnL"] > 0, "PnL"].mean(),
        "AvgLossPnL": trades.loc[trades["PnL"] < 0, "PnL"].mean(),
    }

    summary.update(drawdown_stats(daily))
    summary.update(trade_statistics(trades))
    summary["SortinoRatio"] = sortino_ratio(daily)

    return summary
