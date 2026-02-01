import pyarrow.dataset as ds
import pandas as pd
from pathlib import Path


def load_month_options(parquet_root: str, year: int, month: int) -> pd.DataFrame:
    """
    Loads all option data for a given (year, month) into memory once.
    Automatically handles datetime column/index.
    """

    path = Path(parquet_root) / f"year={year}" / f"month={month:02d}"

    if not path.exists():
        raise FileNotFoundError(f"Options parquet missing: {path}")

    dataset = ds.dataset(path, format="parquet")
    df = dataset.to_table().to_pandas()

    # -------------------------------------------------
    # HANDLE DATETIME ROBUSTLY
    # -------------------------------------------------
    # Case 1: Datetime already index
    if isinstance(df.index, pd.DatetimeIndex):
        pass

    # Case 2: Detect datetime-like column
    else:
        datetime_col = None
        for c in df.columns:
            if c.lower() in ("datetime", "date", "timestamp", "time", "datetime_ist"):
                datetime_col = c
                break

        if datetime_col:
            df[datetime_col] = pd.to_datetime(df[datetime_col])
            df.set_index(datetime_col, inplace=True)

        # Case 3: Date + Time split
        elif {"Date", "Time"}.issubset(df.columns):
            df["Datetime"] = pd.to_datetime(
                df["Date"].astype(str) + " " + df["Time"].astype(str)
            )
            df.set_index("Datetime", inplace=True)

        else:
            raise RuntimeError(
                f"Options data has no datetime info. Columns: {list(df.columns)}"
            )

    df.sort_index(inplace=True)
    return df
