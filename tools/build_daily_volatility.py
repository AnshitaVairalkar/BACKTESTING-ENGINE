from pathlib import Path
import pandas as pd
import numpy as np
# =================================================
# CONFIG
# =================================================
INDEX = "NIFTY"   # or "SENSEX"

# -------------------------------------------------
# PATHS (CORRECT)
# -------------------------------------------------
TOOLS_DIR = Path(__file__).resolve().parent
ENGINE_DIR = TOOLS_DIR.parent
PROJECT_ROOT = ENGINE_DIR.parent

INDEX_PARQUET_MAP = {
    "NIFTY": PROJECT_ROOT / "Index Data" / "NIFTY" / "NIFTY_IDX.parquet",
    "SENSEX": PROJECT_ROOT / "Index Data" / "SENSEX" / "SENSEX_IDX.parquet",
}

OUTPUT_DIR = ENGINE_DIR / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / f"{INDEX.lower()}_daily_volatility.csv"

# =================================================
# LOAD DATA
# =================================================
df = pd.read_parquet(INDEX_PARQUET_MAP[INDEX])
df = df.sort_index()

# =================================================
# DAILY RESAMPLE
# =================================================
daily = df.resample("1D").agg({
    "Open": "first",
    "High": "max",
    "Low": "min",
    "Close": "last"
}).dropna()

# =================================================
# RETURNS & VOL
# =================================================
daily["LogReturn"] = np.log(daily["Close"] / daily["Close"].shift(1))
daily["StdDev"] = daily["LogReturn"].expanding().std(ddof=1)
daily["CalculatedVolatility"] = daily["Close"] * daily["StdDev"]

# =================================================
# OUTPUT
# =================================================
out = daily.reset_index()
out.rename(columns={out.columns[0]: "Date"}, inplace=True)
out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%d-%m-%Y")

out = out[["Date", "Close","LogReturn", "StdDev", "CalculatedVolatility"]]
out.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Volatility file saved at: {OUTPUT_FILE}")
