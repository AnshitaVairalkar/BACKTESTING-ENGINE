from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools


class DynamicATMInventory(BaseStrategy):

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    STRIKE_GAP = 50

    expiry_change_date = "2025-08-28"

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

    def get_strikes(self, spot_price): return {}
    def get_leg_qty(self, leg_id): return 0

    # =================================================

    def on_day_start(self, trade_date, index, market_context):
        self.trade_date = trade_date
        self.day = market_context["day"].upper()
        self.legs = []
        self.leg_counter = itertools.count(1)

        if trade_date < self.expiry_change_date:
            self.R = self.before_expirychange[self.day]
        else:
            self.R = self.after_expirychange[self.day]

    def on_minute(self, ts, index_price):
        actions = []

        if not self.legs and ts.time() >= self.ENTRY_TIME:
            actions += self._new_straddle(index_price)
            return actions

        # ---------- UPSIDE ----------
        while True:
            latest_ce = self._latest("CE")
            if not latest_ce:
                break
            if index_price > latest_ce["upper"]:
                actions.append(self._exit(latest_ce, "UPPER_BREACH"))
                self.legs.remove(latest_ce)
                actions += self._new_straddle(index_price)
            else:
                break

        # ---------- DOWNSIDE ----------
        while True:
            latest_pe = self._latest("PE")
            if not latest_pe:
                break
            if index_price < latest_pe["lower"]:
                actions.append(self._exit(latest_pe, "LOWER_BREACH"))
                self.legs.remove(latest_pe)
                actions += self._new_straddle(index_price)
            else:
                break

        return actions

    def on_day_end(self): pass

    # =================================================

    def _new_straddle(self, index_price):
        atm = round(index_price / self.STRIKE_GAP) * self.STRIKE_GAP
        actions = []

        for opt_type in ("CE", "PE"):
            leg_id = f"L{next(self.leg_counter)}"
            upper = index_price + self.R
            lower = index_price - self.R

            leg = {
                "leg_id": leg_id,
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

    def _latest(self, opt_type):
        for leg in reversed(self.legs):
            if leg["type"] == opt_type:
                return leg
        return None

    def _exit(self, leg, reason):
        return {
            "action": "EXIT",
            "leg_id": leg["leg_id"],
            "reason": reason
        }




####################################    NEW  LOGICC   ########################################################


from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools


class DynamicATMInventory(BaseStrategy):

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    STRIKE_GAP = 50

    expiry_change_date = "2025-08-28"

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
    def get_strikes(self, spot_price): return {}
    def get_leg_qty(self, leg_id): return 0

    # =================================================

    def on_day_start(self, trade_date, index, market_context):
        self.trade_date = trade_date
        self.day = market_context["day"].upper()
        self.legs = []
        self.leg_counter = itertools.count(1)

        if trade_date < self.expiry_change_date:
            self.R = self.before_expirychange[self.day]
        else:
            self.R = self.after_expirychange[self.day]

    def on_minute(self, ts, index_price):
        actions = []

        # Initial entry
        if not self.legs and ts.time() >= self.ENTRY_TIME:
            actions += self._add_straddle(index_price)
            return actions

        exited_any = False

        # ================= UPSIDE: EXIT ALL BREACHED CEs =================
        breached_ces = [
            leg for leg in self.legs
            if leg["type"] == "CE" and index_price > leg["upper"]
        ]

        for leg in breached_ces:
            actions.append(self._exit_leg(leg, "UPPER_BREACH"))
            self.legs.remove(leg)
            exited_any = True

        # ================= DOWNSIDE: EXIT ALL BREACHED PEs =================
        breached_pes = [
            leg for leg in self.legs
            if leg["type"] == "PE" and index_price < leg["lower"]
        ]

        for leg in breached_pes:
            actions.append(self._exit_leg(leg, "LOWER_BREACH"))
            self.legs.remove(leg)
            exited_any = True

        # After exits, add ONE new straddle
        if exited_any:
            actions += self._add_straddle(index_price)

        return actions

    def on_day_end(self):
        pass

    # =================================================

    def _add_straddle(self, index_price):
        atm = round(index_price / self.STRIKE_GAP) * self.STRIKE_GAP
        actions = []

        for opt_type in ("CE", "PE"):
            leg_id = f"L{next(self.leg_counter)}"
            upper = index_price + self.R
            lower = index_price - self.R

            leg = {
                "leg_id": leg_id,
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
