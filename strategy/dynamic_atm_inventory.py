from datetime import time
from strategy.base_strategy import BaseStrategy


class DynamicATMInventory(BaseStrategy):
    """
    Dynamic ATM Inventory Strategy
    Event-driven, stateful strategy
    """

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    SL_PCT = 0.0
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

    # =================================================
    # REQUIRED BY BaseStrategy (NOT USED HERE)
    # =================================================

    def get_strikes(self, spot_price: float):
        """
        Not used for event-driven strategies.
        Implemented only to satisfy BaseStrategy.
        """
        return {}

    def get_leg_qty(self, leg_id: str):
        """
        Not used for event-driven strategies.
        Implemented only to satisfy BaseStrategy.
        """
        return 0

    # =================================================
    # EVENT-DRIVEN LIFECYCLE
    # =================================================

    def on_day_start(self, trade_date, index, market_context):
        self.trade_date = trade_date
        self.day = market_context["day"].upper()
        self.legs = []
        self.last_ref_price = None

        if trade_date < self.expiry_change_date:
            self.R = self.before_expirychange[self.day]
        else:
            self.R = self.after_expirychange[self.day]

    def on_minute(self, timestamp, index_price):
        actions = []

        # Initial entry
        if self.last_ref_price is None and timestamp.time() >= self.ENTRY_TIME:
            self.last_ref_price = index_price
            actions += self._sell_new_straddle(index_price)
            return actions

        if self.last_ref_price is None:
            return []

        upper = self.last_ref_price + self.R
        lower = self.last_ref_price - self.R

        # Upside breach → cut latest CE
        if index_price > upper:
            self._cut_latest("CE")
            self.last_ref_price = index_price
            actions += self._sell_new_straddle(index_price)

        # Downside breach → cut breached PEs only
        elif index_price < lower:
            self._cut_breached_pes(index_price)
            self.last_ref_price = index_price
            actions += self._sell_new_straddle(index_price)

        return actions

    def on_day_end(self):
        self.legs.clear()

    # =================================================
    # INTERNAL HELPERS
    # =================================================

    def _sell_new_straddle(self, index_price):
        atm = round(index_price / self.STRIKE_GAP) * self.STRIKE_GAP
        upper = index_price + self.R
        lower = index_price - self.R

        ce_leg = {
            "option_type": "CE",
            "strike": atm,
            "upper": upper,
            "lower": lower,
        }

        pe_leg = {
            "option_type": "PE",
            "strike": atm,
            "upper": upper,
            "lower": lower,
        }

        self.legs.append(ce_leg)
        self.legs.append(pe_leg)

        return [
            {
                "action": "ENTER",
                "option_type": "CE",
                "strike": atm,
                "qty": -1,
                "ref_price": index_price,
                "upper": upper,
                "lower": lower,
                "range_used": self.R,
                "index_entry": index_price,
            },
            {
                "action": "ENTER",
                "option_type": "PE",
                "strike": atm,
                "qty": -1,
                "ref_price": index_price,
                "upper": upper,
                "lower": lower,
                "range_used": self.R,
                "index_entry": index_price,
            },
        ]

    def _cut_latest(self, opt_type):
        for leg in reversed(self.legs):
            if leg["option_type"] == opt_type:
                self.legs.remove(leg)
                return

    def _cut_breached_pes(self, index_price):
        remaining = []
        for leg in self.legs:
            if leg["option_type"] == "PE" and index_price < leg["lower"]:
                continue
            remaining.append(leg)
        self.legs = remaining
