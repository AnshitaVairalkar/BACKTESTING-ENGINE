from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools
import pandas as pd


class VolatilityStraddles(BaseStrategy):
    """
    Volatility-based ATM Straddle Strategy

    Rules:
    1. Enter ATM CE + ATM PE at 9:20
    2. CE SL = Entry Index + Range
       PE SL = Entry Index - Range
    3. If CE SL hit:
          - Exit old CE
          - Short fresh ATM CE
          - New SL = Current Index + Range
    4. If PE SL hit:
          - Exit old PE
          - Short fresh ATM PE
          - New SL = Current Index - Range
    5. At ALL times only 2 legs are open
    6. Exit all positions at 15:20
    """

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    STRIKE_GAP = 50

    def __init__(self, volatility_csv_path: str = None):
        self.volatility_csv_path = volatility_csv_path
        self.volatility_map = {}

        if volatility_csv_path:
            self._load_volatility_data()

    def _load_volatility_data(self):
        df = pd.read_csv(self.volatility_csv_path)
        df["Date"] = pd.to_datetime(
            df["Date"], format="%d-%m-%Y"
        ).dt.strftime("%Y-%m-%d")

        self.volatility_map = dict(
            zip(df["Date"], df["CalculatedVolatility"])
        )

    # Required by BaseStrategy
    def get_strikes(self, spot_price):
        return {}

    def get_leg_qty(self, leg_id):
        return 0

    # =================================================
    # EVENT METHODS
    # =================================================

    def on_day_start(self, trade_date, index, market_context):

        self.trade_date = trade_date
        self.index = index
        self.market_context = market_context

        self.legs = []
        self.leg_counter = itertools.count(1)
        self.initial_index_open = None

        self.volatility = self._get_volatility_for_date(trade_date)

        if self.volatility is None:
            raise ValueError(f"No volatility data for {trade_date}")

    def _get_volatility_for_date(self, trade_date):

        if trade_date in self.volatility_map:
            return self.volatility_map[trade_date]

        dates = sorted(
            [d for d in self.volatility_map.keys() if d < trade_date]
        )

        if dates:
            return self.volatility_map[dates[-1]]

        return None

    def on_minute(self, ts, index_price):

        actions = []
        candle_time = ts.time()

        # ===== END OF DAY EXIT =====
        if candle_time >= self.EXIT_TIME and self.legs:
            for leg in list(self.legs):
                actions.append(self._exit_leg(leg, "DAY_END"))
                self.legs.remove(leg)
            return actions

        # ===== INITIAL ENTRY =====
        if not self.legs and candle_time >= self.ENTRY_TIME:
            if self.initial_index_open is None:
                self.initial_index_open = index_price

            return self._create_initial_straddle(self.initial_index_open)

        # ===== CHECK BREACHES =====
        for leg in list(self.legs):

            # CE SL hit
            if leg["type"] == "CE" and index_price > leg["sl_index"]:

                # Exit old CE
                actions.append(self._exit_leg(leg, "CE_SL_HIT"))
                self.legs.remove(leg)

                # Enter fresh ATM CE
                actions += self._create_new_atm_leg("CE", index_price)

                break  # Only one replacement per candle

            # PE SL hit
            if leg["type"] == "PE" and index_price < leg["sl_index"]:

                # Exit old PE
                actions.append(self._exit_leg(leg, "PE_SL_HIT"))
                self.legs.remove(leg)

                # Enter fresh ATM PE
                actions += self._create_new_atm_leg("PE", index_price)

                break

        return actions

    # =================================================
    # HELPERS
    # =================================================

    def _create_initial_straddle(self, index_price):

        actions = []

        atm_strike = self._round_strike(index_price)

        # CE
        ce_sl = index_price + self.volatility

        ce_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": "CE",
            "strike": atm_strike,
            "entry_index_price": index_price,
            "sl_index": ce_sl,
            "volatility": self.volatility
        }

        self.legs.append(ce_leg)

        actions.append({
            "action": "ENTER",
            "leg_id": ce_leg["leg_id"],
            "type": "CE",
            "strike": atm_strike,
            "entry_index_price": index_price,
            "sl_index": ce_sl,
            "R": self.volatility
        })

        # PE
        pe_sl = index_price - self.volatility

        pe_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": "PE",
            "strike": atm_strike,
            "entry_index_price": index_price,
            "sl_index": pe_sl,
            "volatility": self.volatility
        }

        self.legs.append(pe_leg)

        actions.append({
            "action": "ENTER",
            "leg_id": pe_leg["leg_id"],
            "type": "PE",
            "strike": atm_strike,
            "entry_index_price": index_price,
            "sl_index": pe_sl,
            "R": self.volatility
        })

        return actions

    def _create_new_atm_leg(self, opt_type, current_index_price):

        actions = []

        new_atm_strike = self._round_strike(current_index_price)

        if opt_type == "CE":
            new_sl = current_index_price + self.volatility
        else:
            new_sl = current_index_price - self.volatility

        new_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": opt_type,
            "strike": new_atm_strike,
            "entry_index_price": current_index_price,
            "sl_index": new_sl,
            "volatility": self.volatility
        }

        self.legs.append(new_leg)

        actions.append({
            "action": "ENTER",
            "leg_id": new_leg["leg_id"],
            "type": opt_type,
            "strike": new_atm_strike,
            "entry_index_price": current_index_price,
            "sl_index": new_sl,
            "R": self.volatility
        })

        return actions

    def _exit_leg(self, leg, reason):

        return {
            "action": "EXIT",
            "leg_id": leg["leg_id"],
            "reason": reason
        }

    def _round_strike(self, price):

        return round(price / self.STRIKE_GAP) * self.STRIKE_GAP