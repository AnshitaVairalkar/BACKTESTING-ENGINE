import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pathlib import Path
from typing import Optional
from datetime import datetime

# Two-level cache:
# L1: (trade_date, expiry_date)          -> full expiry DataFrame
# L2: (trade_date, expiry_date, strike, type) -> filtered leg DataFrame
#     Values are dicts of {time -> Close} for O(1) minute lookup
_EXPIRY_CACHE = {}
_LEG_CACHE = {}       # { cache_key: leg_df }
_LEG_TIME_IDX = {}    # { cache_key: { time_obj: row_series } }


def _build_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        return df
    
    if 'ts' in df.columns:
        df['DateTime'] = pd.to_datetime(df['ts'], unit='ms')
    elif 'date' in df.columns and 'time' in df.columns:
        df['DateTime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['time'], unit='ms')
    else:
        raise ValueError(f"Cannot build DateTime index. Columns: {list(df.columns)}")
    
    df.set_index('DateTime', inplace=True)
    df.sort_index(inplace=True)
    return df


def _load_expiry_data(parquet_root, trade_date, expiry_date):
    expiry_str = expiry_date.strftime('%Y-%m-%d')
    months_to_try = [(expiry_date.year, expiry_date.month)]
    if (trade_date.year, trade_date.month) not in months_to_try:
        months_to_try.append((trade_date.year, trade_date.month))
    
    for year, month in months_to_try:
        dataset_path = (
            Path(parquet_root) /
            f"year={year}" /
            f"month={month:02d}" /
            f"expiry={expiry_str}"
        )
        if not dataset_path.exists():
            continue
        try:
            dataset = ds.dataset(dataset_path, format="parquet")
            trade_date_scalar = pa.scalar(trade_date, type=pa.date32())
            table = dataset.to_table(filter=(ds.field("date") == trade_date_scalar))
            if table.num_rows == 0:
                continue
            df = table.to_pandas()
            df["StrikePrice"] = df["StrikePrice"].astype(int)
            df["Type"] = df["Type"].astype(str)
            df = _build_datetime_index(df)
            return df
        except Exception as e:
            if (year, month) == months_to_try[-1]:
                print(f"Warning: Failed to load {dataset_path}: {e}")
            continue
    return None


def load_option_data(
    parquet_root: str,
    trade_date: str,
    expiry: pd.Timestamp,
    strike: int,
    option_type: str
) -> pd.DataFrame:
    trade_date = pd.Timestamp(trade_date).date()
    expiry_date = expiry.date()

    # L1: expiry-level cache
    expiry_key = (trade_date, expiry_date)
    if expiry_key not in _EXPIRY_CACHE:
        expiry_df = _load_expiry_data(parquet_root, trade_date, expiry_date)
        if expiry_df is None:
            raise ValueError(f"No option data for {trade_date} | Expiry {expiry_date}")
        _EXPIRY_CACHE[expiry_key] = expiry_df

    # L2: leg-level cache (avoids re-filtering every call)
    leg_key = (trade_date, expiry_date, strike, option_type)
    if leg_key not in _LEG_CACHE:
        expiry_df = _EXPIRY_CACHE[expiry_key]
        leg_df = expiry_df[
            (expiry_df["StrikePrice"] == strike) &
            (expiry_df["Type"] == option_type)
        ].copy()
        if leg_df.empty:
            raise ValueError(
                f"No option data for {trade_date} | {option_type} {strike} | Expiry {expiry_date}"
            )
        _LEG_CACHE[leg_key] = leg_df

        # Build time -> row index for O(1) minute lookup
        _LEG_TIME_IDX[leg_key] = {t: i for i, t in enumerate(leg_df.index.time)}

    return _LEG_CACHE[leg_key]


def get_close_at_time(
    parquet_root: str,
    trade_date,
    expiry_date,
    strike: int,
    option_type: str,
    candle_time
) -> Optional[float]:
    """
    O(1) close price lookup for a specific minute.
    Returns None if candle missing (tracker uses this).
    """
    # Normalize types to match keys built in load_option_data
    if isinstance(trade_date, str):
        trade_date = pd.Timestamp(trade_date).date()
    if isinstance(expiry_date, str):
        expiry_date = pd.Timestamp(expiry_date).date()
    leg_key = (trade_date, expiry_date, strike, option_type)
    time_idx = _LEG_TIME_IDX.get(leg_key)
    if time_idx is None:
        return None
    row_i = time_idx.get(candle_time)
    if row_i is None:
        return None
    return float(_LEG_CACHE[leg_key].iloc[row_i]["Close"])


def clear_cache():
    global _EXPIRY_CACHE, _LEG_CACHE, _LEG_TIME_IDX
    _EXPIRY_CACHE.clear()
    _LEG_CACHE.clear()
    _LEG_TIME_IDX.clear()


def get_cache_stats():
    return {
        "cached_expiries": len(_EXPIRY_CACHE),
        "cached_legs": len(_LEG_CACHE),
    }