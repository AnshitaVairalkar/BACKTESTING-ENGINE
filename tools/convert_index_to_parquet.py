import pandas as pd

xlsx_path = "../Index Data/SENSEX/SENSEX_IDX.xlsx"
parquet_path = "../Index Data/SENSEX/SENSEX_IDX.parquet"

df = pd.read_excel(xlsx_path)

# Normalize DateTime once
df["DateTime"] = pd.to_datetime(
    df["Date"].astype(str) + " " + df["Time"]
)

df.drop(columns=["Date", "Time"], inplace=True)
df.set_index("DateTime", inplace=True)
df.sort_index(inplace=True)

df.to_parquet(parquet_path, compression="zstd")

print("Saved:", parquet_path)
