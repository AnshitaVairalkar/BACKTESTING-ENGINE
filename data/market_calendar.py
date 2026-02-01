import pandas as pd
from typing import Optional


class MarketCalendarStore:
    """Singleton store for market calendar data"""
    _df: Optional[pd.DataFrame] = None
    _loaded_path: Optional[str] = None

    @classmethod
    def load(cls, calendar_csv_path: str):
        """Load market calendar if not already loaded"""
        if cls._df is None or cls._loaded_path != calendar_csv_path:
            cls._df = pd.read_csv(
                calendar_csv_path,
                parse_dates=["Date", "ExpiryDate"]
            )
            cls._df["Date"] = cls._df["Date"].dt.date
            cls._loaded_path = calendar_csv_path

    @classmethod
    def get_day(cls, trade_date: str) -> dict:
        """Get market context for a specific trading day"""
        if cls._df is None:
            raise RuntimeError("MarketCalendarStore not loaded. Call load() first.")
        
        trade_date = pd.Timestamp(trade_date).date()
        row = cls._df[cls._df["Date"] == trade_date]

        if row.empty:
            raise ValueError(f"No market calendar entry for {trade_date}")

        row = row.iloc[0]

        return {
            "weekly_expiry": row["ExpiryDate"],
            "dte_weekly": int(row["DTE_CurrentWeek"]),
            "monthly_expiry": row.get("MonthlyExpiry", None),
            "day": row.get("Day", None),
        }

    @classmethod
    def get_all_dates(cls) -> list:
        """Get all available trading dates"""
        if cls._df is None:
            raise RuntimeError("MarketCalendarStore not loaded. Call load() first.")
        
        return cls._df["Date"].tolist()

    @classmethod
    def clear(cls):
        """Clear cached data"""
        cls._df = None
        cls._loaded_path = None


def get_market_context(calendar_csv_path: str, trade_date: str) -> dict:
    """
    Get market context for a specific trading date.
    
    Args:
        calendar_csv_path: Path to market calendar CSV
        trade_date: Trading date in YYYY-MM-DD format
        
    Returns:
        Dictionary with expiry dates and DTE information
    """
    MarketCalendarStore.load(calendar_csv_path)
    return MarketCalendarStore.get_day(trade_date)