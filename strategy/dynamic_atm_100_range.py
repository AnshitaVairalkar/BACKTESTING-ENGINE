from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools


class DynamicATM100Range(BaseStrategy):

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    STRIKE_GAP = 50
    R = 100  # Fixed 100-point range for all days

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
        self.processed_levels = set()  # track which ATM levels we've already entered

    # =================================================

    def on_minute(self, ts, index_price):
        actions = []

        # ---------------- Initial Entry ----------------
        if not self.straddles and ts.time() >= self.ENTRY_TIME:
            actions += self._add_straddle(index_price)
            return actions

        # =================================================
        # ITERATIVE BREACH PROCESSING
        # Keep checking and adding until no more breaches
        # =================================================
        max_iterations = 20  # Safety limit to prevent infinite loops
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # STEP A: Find and exit ALL currently breached legs
            exited_legs = []
            breached_levels = set()

            for leg in list(self.legs):
                if leg["type"] == "CE" and index_price > leg["upper"]:
                    actions.append(self._exit_leg(leg, "UPPER_BREACH"))
                    exited_legs.append(leg)
                    breached_levels.add(leg["upper"])  # The SL level that was breached

                elif leg["type"] == "PE" and index_price < leg["lower"]:
                    actions.append(self._exit_leg(leg, "LOWER_BREACH"))
                    exited_legs.append(leg)
                    breached_levels.add(leg["lower"])  # The SL level that was breached

            # Remove exited legs
            for leg in exited_legs:
                self.legs.remove(leg)

            # STEP B: Add new straddles at breached levels
            new_entries = []
            for breached_level in breached_levels:
                # Round to nearest 50 (should already be at 50 strike)
                atm = round(breached_level / self.STRIKE_GAP) * self.STRIKE_GAP
                
                # Only add if we haven't already entered this level
                if atm not in self.processed_levels:
                    new_entries += self._add_straddle_at_strike(atm, index_price)
            
            actions += new_entries

            # If no new entries were made, we're done
            if not new_entries:
                break

        return actions

    def on_day_end(self):
        pass

    # =================================================
    # INTERNAL HELPERS
    # =================================================

    def _add_straddle(self, index_price):
        """Add straddle based on current index price (for initial entry)"""
        atm = round(index_price / self.STRIKE_GAP) * self.STRIKE_GAP
        return self._add_straddle_at_strike(atm, index_price)

    def _add_straddle_at_strike(self, atm, index_price):
        """Add straddle at specific strike level"""
        
        # Mark this level as processed
        self.processed_levels.add(atm)
        
        actions = []

        upper = atm + self.R
        lower = atm - self.R

        straddle_id = f"S{next(self.straddle_counter)}"

        straddle = {
            "straddle_id": straddle_id,
            "atm": atm,
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