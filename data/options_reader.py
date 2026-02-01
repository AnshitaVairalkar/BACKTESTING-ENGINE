import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pathlib import Path
from typing import Optional
from datetime import datetime

# Global cache: {(trade_date, expiry_date): DataFrame}
_EXPIRY_CACHE = {}


def _build_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build DateTime index from ts (timestamp in milliseconds).
    Handles both integer timestamps and existing datetime columns.
    """
    if isinstance(df.index, pd.DatetimeIndex):
        return df
    
    if 'ts' in df.columns:
        # Convert millisecond timestamp to datetime
        df['DateTime'] = pd.to_datetime(df['ts'], unit='ms')
    elif 'date' in df.columns and 'time' in df.columns:
        # Fallback: combine date + time (time in milliseconds)
        df['DateTime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['time'], unit='ms')
    else:
        raise ValueError(f"Cannot build DateTime index. Columns: {list(df.columns)}")
    
    df.set_index('DateTime', inplace=True)
    df.sort_index(inplace=True)
    
    return df


def _load_expiry_data(
    parquet_root: str,
    trade_date,
    expiry_date
) -> Optional[pd.DataFrame]:
    """
    Load all option data for a specific trade_date and expiry_date.
    Returns DataFrame with all strikes/types, or None if not found.
    
    Structure: year=YYYY/month=MM/expiry=YYYY-MM-DD/
    
    ✅ FIX: Try BOTH trade month and expiry month to handle cross-month cases
    """
    expiry_str = expiry_date.strftime('%Y-%m-%d')
    
    # ✅ Strategy: Try EXPIRY month first (most common case)
    # Then try TRADE month as fallback
    months_to_try = []
    
    # Priority 1: Expiry month (most data is organized this way)
    months_to_try.append((expiry_date.year, expiry_date.month))
    
    # Priority 2: Trade month (for cross-month edge cases)
    if (trade_date.year, trade_date.month) not in months_to_try:
        months_to_try.append((trade_date.year, trade_date.month))
    
    for year, month in months_to_try:
        # Build path: year=YYYY/month=MM/expiry=YYYY-MM-DD
        dataset_path = (
            Path(parquet_root) / 
            f"year={year}" / 
            f"month={month:02d}" / 
            f"expiry={expiry_str}"
        )
        
        if not dataset_path.exists():
            continue
        
        try:
            # Load entire expiry folder
            dataset = ds.dataset(dataset_path, format="parquet")
            
            # ✅ FIX: Convert string date to proper date32 scalar for PyArrow filter
            trade_date_scalar = pa.scalar(trade_date, type=pa.date32())
            
            # Filter by trade_date using proper date type
            table = dataset.to_table(
                filter=(ds.field("date") == trade_date_scalar)
            )
            
            if table.num_rows == 0:
                continue  # Try next month
            
            df = table.to_pandas()
            
            # Normalize types
            df["StrikePrice"] = df["StrikePrice"].astype(int)
            df["Type"] = df["Type"].astype(str)
            
            # Build DateTime index
            df = _build_datetime_index(df)
            
            return df
            
        except Exception as e:
            # Only print warning if this is the last attempt
            if (year, month) == months_to_try[-1]:
                print(f"Warning: Failed to load {dataset_path}: {e}")
            continue
    
    # Not found in any month
    return None


def load_option_data(
    parquet_root: str,
    trade_date: str,
    expiry: pd.Timestamp,
    strike: int,
    option_type: str
) -> pd.DataFrame:
    """
    Load option data for specific strike and type.
    Uses caching at expiry level for performance.
    
    Args:
        parquet_root: Root path to options parquet data
        trade_date: Trading date (YYYY-MM-DD)
        expiry: Expiry timestamp
        strike: Strike price
        option_type: 'CE' or 'PE'
        
    Returns:
        DataFrame with DateTime index and OHLCV columns
    """
    trade_date = pd.Timestamp(trade_date).date()
    expiry_date = expiry.date()
    
    # -------------------------------------------------
    # CACHE KEY (EXPIRY LEVEL)
    # -------------------------------------------------
    cache_key = (trade_date, expiry_date)
    
    if cache_key not in _EXPIRY_CACHE:
        expiry_df = _load_expiry_data(
            parquet_root=parquet_root,
            trade_date=trade_date,
            expiry_date=expiry_date
        )
        
        if expiry_df is None:
            raise ValueError(
                f"No option data for {trade_date} | Expiry {expiry_date}"
            )
        
        _EXPIRY_CACHE[cache_key] = expiry_df
    
    expiry_df = _EXPIRY_CACHE[cache_key]
    
    # -------------------------------------------------
    # FILTER BY STRIKE + TYPE
    # -------------------------------------------------
    leg_df = expiry_df[
        (expiry_df["StrikePrice"] == strike) &
        (expiry_df["Type"] == option_type)
    ]
    
    if leg_df.empty:
        raise ValueError(
            f"No option data for {trade_date} | "
            f"{option_type} {strike} | Expiry {expiry_date}"
        )
    
    return leg_df.copy()


def clear_cache():
    """Clear the options data cache"""
    global _EXPIRY_CACHE
    _EXPIRY_CACHE.clear()


def get_cache_stats():
    """Return cache statistics"""
    return {
        "cached_expiries": len(_EXPIRY_CACHE),
        "cache_keys": list(_EXPIRY_CACHE.keys())
    }