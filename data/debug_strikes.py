import pyarrow.dataset as ds
import pandas as pd
from pathlib import Path

PARQUET_ROOT = "../Options_Parquet/SENSEX"
TRADE_DATE = pd.Timestamp("2023-08-10").date()
EXPIRY = pd.Timestamp("2023-08-11").date()

dataset_path = Path(PARQUET_ROOT) / "year=2023" / "month=08"
dataset = ds.dataset(dataset_path, format="parquet")

df = dataset.to_table().to_pandas()

df["date"] = pd.to_datetime(df["date"]).dt.date
df["ExpiryDate"] = pd.to_datetime(df["ExpiryDate"]).dt.date
df["StrikePrice"] = df["StrikePrice"].astype(int)

day_df = df[
    (df["date"] == TRADE_DATE) &
    (df["ExpiryDate"] == EXPIRY)
]

print("Total rows:", len(day_df))

print("\nUnique CE strikes:")
print(sorted(day_df[day_df["Type"] == "CE"]["StrikePrice"].unique()))

print("\nUnique PE strikes:")
print(sorted(day_df[day_df["Type"] == "PE"]["StrikePrice"].unique()))
