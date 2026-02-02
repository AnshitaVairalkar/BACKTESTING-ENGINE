# import pandas as pd

# xlsx_path = "C:\\Users\\anshu\\Documents\\PERSONAL\\PROJECTS\\STRATEGIES\\SENSEX ITM - Copy\\Index Data\\NIFTY\\NIFTY_IDX.xlsx"
# parquet_path = "C:\\Users\\anshu\\Documents\\PERSONAL\\PROJECTS\\STRATEGIES\\SENSEX ITM - Copy\\Index Data\\NIFTY\\NIFTY_IDX.parquet"

# df = pd.read_excel(xlsx_path)

# # Normalize DateTime once
# df["DateTime"] = pd.to_datetime(
#     df["Date"].astype(str) + " " + df["Time"]
# )

# df.drop(columns=["Date", "Time"], inplace=True)
# df.set_index("DateTime", inplace=True)
# df.sort_index(inplace=True)

# df.to_parquet(parquet_path, compression="zstd")

# print("Saved:", parquet_path)



import pandas as pd

# File paths
xlsx_path = r"C:\Users\anshu\Documents\PERSONAL\PROJECTS\STRATEGIES\SENSEX ITM - Copy\Index Data\NIFTY\NIFTY_IDX.xlsx"
parquet_path = r"C:\Users\anshu\Documents\PERSONAL\PROJECTS\STRATEGIES\SENSEX ITM - Copy\Index Data\NIFTY\NIFTY_IDX.parquet"

# Read Excel
df = pd.read_excel(xlsx_path)

# Clean column names (safety, no logic change)
df.columns = df.columns.str.strip()

# Create DateTime column (same logic, safer casting)
df["DateTime"] = pd.to_datetime(
    df["Date"].astype(str) + " " + df["Time"].astype(str),
    errors="raise"
)

# Drop old columns
df.drop(columns=["Date", "Time"], inplace=True)

# Set index and sort
df.set_index("DateTime", inplace=True)
df.sort_index(inplace=True)

# Save as parquet
df.to_parquet(
    parquet_path,
    engine="pyarrow",
    compression="zstd"
)

print("Saved:", parquet_path)
