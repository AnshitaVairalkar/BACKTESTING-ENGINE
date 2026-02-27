from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools


class DynamicATMInventory(BaseStrategy):

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    STRIKE_GAP = 50

    expiry_change_date = "2025-08-28"
    # nifty points
    before_expirychange = {
        "FRIDAY": 85.3,
        "MONDAY": 77.4,
        "TUESDAY": 128,
        "WEDNESDAY": 83.8,
        "THURSDAY": 79.9,
    }

    after_expirychange = {
        "WEDNESDAY": 85.3,
        "THURSDAY": 77.4,
        "FRIDAY": 128,
        "MONDAY": 83.8,
        "TUESDAY": 79.9,
    }



    # Required by BaseStrategy
    def get_strikes(self, spot_price): 
        return {}

    def get_leg_qty(self, leg_id): 
        return 0

    # =================================================

    def on_day_start(self, trade_date, index, market_context):
        self.trade_date = trade_date
        self.day = market_context["day"].upper()

        self.legs = []               # all open legs
        self.straddles = []          # ordered list of straddles (levels)
        self.leg_counter = itertools.count(1)
        self.straddle_counter = itertools.count(1)

        if trade_date < self.expiry_change_date:
            self.R = self.before_expirychange[self.day]
        else:
            self.R = self.after_expirychange[self.day]

    # =================================================

    def on_minute(self, ts, index_price):
        actions = []

        # ---------------- Initial Entry ----------------
        if not self.straddles and ts.time() >= self.ENTRY_TIME:
            actions += self._add_straddle(index_price)
            return actions

        # =================================================
        # STEP A: CLEANUP (exit ANY breached legs)
        # =================================================
        exited_legs = []

        for leg in list(self.legs):
            if leg["type"] == "CE" and index_price > leg["upper"]:
                actions.append(self._exit_leg(leg, "UPPER_BREACH"))
                exited_legs.append(leg)

            elif leg["type"] == "PE" and index_price < leg["lower"]:
                actions.append(self._exit_leg(leg, "LOWER_BREACH"))
                exited_legs.append(leg)

        for leg in exited_legs:
            self.legs.remove(leg)

        # =================================================
        # STEP B: ENTRY DECISION (ONLY latest level)
        # =================================================
        if not self.straddles:
            return actions

        latest = self.straddles[-1]

        latest_upper = latest["upper"]
        latest_lower = latest["lower"]

        if index_price > latest_upper or index_price < latest_lower:
            actions += self._add_straddle(index_price)

        return actions

    def on_day_end(self):
        pass

    # =================================================
    # INTERNAL HELPERS
    # =================================================

    def _add_straddle(self, index_price):
        atm = round(index_price / self.STRIKE_GAP) * self.STRIKE_GAP
        actions = []

        upper = index_price + self.R
        lower = index_price - self.R

        straddle_id = f"S{next(self.straddle_counter)}"

        straddle = {
            "straddle_id": straddle_id,
            "ref_price": index_price,
            "upper": upper,
            "lower": lower,
        }

        self.straddles.append(straddle)

        for opt_type in ("CE", "PE"):
            leg_id = f"L{next(self.leg_counter)}"

            leg = {
                "leg_id": leg_id,
                "straddle_id": straddle_id,
                "type": opt_type,
                "strike": atm,
                "ref_price": index_price,
                "upper": upper,
                "lower": lower,
                "R": self.R
            }

            self.legs.append(leg)

            actions.append({
                "action": "ENTER",
                "leg_id": leg_id,
                "type": opt_type,
                "strike": atm,
                "ref_price": index_price,
                "upper": upper,
                "lower": lower,
                "R": self.R
            })

        return actions

    def _exit_leg(self, leg, reason):
        return {
            "action": "EXIT",
            "leg_id": leg["leg_id"],
            "reason": reason
        }
