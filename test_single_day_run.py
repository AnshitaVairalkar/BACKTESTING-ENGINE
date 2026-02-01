from engine.backtest_engine import run_single_day_backtest
from pathlib import Path

# --------------------
# PATH CONFIG
# --------------------
ROOT = Path(__file__).resolve().parent

INDEX_PARQUET = ROOT / "../Index Data/SENSEX/SENSEX_IDX.parquet"
CALENDAR_CSV = ROOT / "../ExpiryDates/SENSEX_market_dates.csv"
OPTIONS_PARQUET = ROOT / "../Options_Parquet/SENSEX"

TRADE_DATE = "2023-08-10"   # pick a known trading day

# --------------------
# RUN BACKTEST
# --------------------
trades = run_single_day_backtest(
    trade_date=TRADE_DATE,
    index_parquet=str(INDEX_PARQUET),
    calendar_csv=str(CALENDAR_CSV),
    options_parquet_root=str(OPTIONS_PARQUET),
)

# --------------------
# OUTPUT VALIDATION
# --------------------
print(f"\nBacktest results for {TRADE_DATE}\n")

for t in trades:
    print("-" * 60)
    for k, v in t.items():
        print(f"{k:15}: {v}")
