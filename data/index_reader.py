import pandas as pd
from typing import Optional


class IndexDataStore:
    """Singleton store for index data with efficient caching"""
    _df: Optional[pd.DataFrame] = None
    _loaded_path: Optional[str] = None

    @classmethod
    def load(cls, parquet_path: str):
        """Load index data if not already loaded"""
        if cls._df is None or cls._loaded_path != parquet_path:
            cls._df = pd.read_parquet(parquet_path)
            cls._df.sort_index(inplace=True)
            cls._loaded_path = parquet_path

    @classmethod
    def get_day(cls, trade_date: str) -> pd.DataFrame:
        """Get index data for a specific trading day"""
        if cls._df is None:
            raise RuntimeError("IndexDataStore not loaded. Call load() first.")
        
        try:
            day_df = cls._df.loc[trade_date]
        except KeyError:
            raise ValueError(f"No index data for {trade_date}")
        
        if day_df.empty:
            raise ValueError(f"No index data for {trade_date}")
        
        return day_df

    @classmethod
    def get_all_dates(cls) -> list:
        """Get all available trading dates"""
        if cls._df is None:
            raise RuntimeError("IndexDataStore not loaded. Call load() first.")
        
        return cls._df.index.normalize().unique().strftime("%Y-%m-%d").tolist()

    @classmethod
    def clear(cls):
        """Clear cached data"""
        cls._df = None
        cls._loaded_path = None


def read_index_data(parquet_path: str, trade_date: str) -> pd.DataFrame:
    """
    Read index data for a specific trading date.
    
    Args:
        parquet_path: Path to index parquet file
        trade_date: Trading date in YYYY-MM-DD format
        
    Returns:
        DataFrame with DateTime index and OHLC columns
    """
    IndexDataStore.load(parquet_path)
    return IndexDataStore.get_day(trade_date)