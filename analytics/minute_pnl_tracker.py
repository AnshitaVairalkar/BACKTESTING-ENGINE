"""
Minute PnL Tracker
==================
PnL at each minute = realized PnL (closed legs) + MTM PnL (open legs at close).

Output:
  output/1min_pnl/<filename>.csv         e.g. nifty_volatilitystrangles_20210601_20251231.csv
  output/1min_pnl/<filename>_issues.csv
"""

import pandas as pd
from pathlib import Path
from data.options_reader import get_close_at_time


class MinutePnLTracker:

    def __init__(self, filename: str, output_dir: Path):
        """
        filename: used for CSV name, e.g. nifty_volatilitystrangles_20210601_20251231
        Strategy name is written into the Strategy column of each row.
        """
        self.filename = filename
        self.output_dir = Path(output_dir) / "1min_pnl"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.pnl_rows = []
        self.issue_rows = []
        self._realized_pnl = 0.0
        self._strategy_name = None  # set on new_day

    def new_day(self, trade_date: str, strategy_name: str):
        self._realized_pnl = 0.0
        self._strategy_name = strategy_name

    def add_realized(self, pnl: float):
        self._realized_pnl += pnl

    def record(self, ts, trade_date, open_legs: dict, market: dict, expiry_date):
        candle_time = ts.time()
        time_str = candle_time.strftime("%H:%M")
        mtm_pnl = 0.0
        missing = []

        for leg_id, leg in open_legs.items():
            meta = leg["meta"]
            strike = meta["strike"]
            opt_type = meta["type"]
            entry_price = leg["entry_price"]

            close = get_close_at_time(
                parquet_root=None,
                trade_date=trade_date,
                expiry_date=expiry_date,
                strike=strike,
                option_type=opt_type,
                candle_time=candle_time
            )

            if close is None:
                missing.append({
                    "Date": trade_date,
                    "Time": time_str,
                    "Strategy": self._strategy_name,
                    "LegID": leg_id,
                    "Strike": strike,
                    "Type": opt_type,
                    "Issue": f"Close candle missing at {time_str}"
                })
                continue

            mtm_pnl += entry_price - close

        if missing:
            self.issue_rows.extend(missing)
            return

        self.pnl_rows.append({
            "Date": trade_date,
            "Time": time_str,
            "Strategy": self._strategy_name,
            "PnL": round(self._realized_pnl + mtm_pnl, 4)
        })

    def save(self):
        pnl_file = self.output_dir / f"{self.filename}.csv"
        issues_file = self.output_dir / f"{self.filename}_issues.csv"

        if self.pnl_rows:
            new_df = pd.DataFrame(self.pnl_rows, columns=["Date", "Time", "Strategy", "PnL"])
            if pnl_file.exists():
                existing = pd.read_csv(pnl_file)
                new_df = pd.concat([existing, new_df], ignore_index=True)
            new_df.to_csv(pnl_file, index=False)
            print(f"  📈 1min PnL saved → {pnl_file}  ({len(self.pnl_rows)} rows)")

        if self.issue_rows:
            new_df = pd.DataFrame(self.issue_rows)
            if issues_file.exists():
                existing = pd.read_csv(issues_file)
                new_df = pd.concat([existing, new_df], ignore_index=True)
            new_df.to_csv(issues_file, index=False)
            print(f"  ⚠️  Issues saved    → {issues_file}  ({len(self.issue_rows)} rows)")

        self.pnl_rows = []
        self.issue_rows = []