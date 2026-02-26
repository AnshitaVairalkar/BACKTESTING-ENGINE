from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools
import pandas as pd


class VolatilityStrangles(BaseStrategy):
    """
    Volatility-based Strangle Strategy
    
    Logic:
    1. Use previous day's calculated volatility as the range
    2. At 9:20, enter CE and PE at (spot ± volatility)
    3. If index breaches CE_SL or PE_SL, exit that leg and enter new one
    4. Track exact SL levels (before rounding) for each leg
    
    Exit on CLOSE, Entry on next candle's OPEN
    """

    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 20)
    STRIKE_GAP = 50  # Adjust to 100 if needed

    def __init__(self, volatility_csv_path: str = None):
        """
        Args:
            volatility_csv_path: Path to CSV with columns [Date, Calculated_Volatility]
        """
        self.volatility_csv_path = volatility_csv_path
        self.volatility_map = {}
        
        if volatility_csv_path:
            self._load_volatility_data()

    def _load_volatility_data(self):
        """Load volatility data from CSV"""
        df = pd.read_csv(self.volatility_csv_path)
        # Parse dates in DD-MM-YYYY format and convert to YYYY-MM-DD
        df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y").dt.strftime("%Y-%m-%d")
        self.volatility_map = dict(zip(df["Date"], df["CalculatedVolatility"]))

    # Required by BaseStrategy (not used in event-driven mode)
    def get_strikes(self, spot_price): 
        return {}

    def get_leg_qty(self, leg_id): 
        return 0

    # =================================================
    # EVENT-DRIVEN METHODS
    # =================================================

    def on_day_start(self, trade_date, index, market_context):
        """Initialize strategy state for the day"""
        self.trade_date = trade_date
        self.index = index
        self.market_context = market_context
        
        self.legs = []  # Active legs
        self.leg_counter = itertools.count(1)
        self.initial_index_open = None  # Store 9:20 open price
        
        # Get volatility for this day (from previous day's data)
        self.volatility = self._get_volatility_for_date(trade_date)
        
        if self.volatility is None:
            raise ValueError(f"No volatility data for {trade_date}")


    def _get_volatility_for_date(self, trade_date):
        """
        Get calculated volatility for the trade date.
        This should be the previous day's calculated volatility.
        """
        # Try exact date first
        if trade_date in self.volatility_map:
            return self.volatility_map[trade_date]
        
        # Otherwise, find most recent previous date
        dates = sorted([d for d in self.volatility_map.keys() if d < trade_date])
        if dates:
            prev_date = dates[-1]
            return self.volatility_map[prev_date]
        
        return None

    def on_minute(self, ts, index_price):
        """Process each minute's index price"""
        actions = []
        candle_time = ts.time()

        # ================== INITIAL ENTRY ==================
        if not self.legs and candle_time >= self.ENTRY_TIME:
            # Store 9:20 open for INDEX_ENTRY_PRICE (this is the OPEN price at entry)
            if self.initial_index_open is None:
                self.initial_index_open = index_price
            actions += self._create_initial_strangle(self.initial_index_open)
            return actions

        # ================== CHECK FOR BREACHES ==================
        exited_legs = []
        
        for leg in list(self.legs):
            breached = False
            breach_reason = None
            
            if leg["type"] == "CE" and index_price > leg["sl_index"]:
                breached = True
                breach_reason = "CE_SL_HIT"
                
            elif leg["type"] == "PE" and index_price < leg["sl_index"]:
                breached = True
                breach_reason = "PE_SL_HIT"
            
            if breached:
                # Exit this leg
                actions.append(self._exit_leg(leg, breach_reason))
                exited_legs.append(leg)
                
                # Create new leg with adjusted SL
                actions += self._create_new_leg(leg, index_price)

        # Remove exited legs
        for leg in exited_legs:
            self.legs.remove(leg)

        return actions

    def on_day_end(self):
        """Cleanup at end of day"""
        pass

    # =================================================
    # INTERNAL HELPERS
    # =================================================

    def _create_initial_strangle(self, index_price):
        """Create initial CE and PE positions at 9:20"""
        actions = []
        
        # CE leg: index_price + volatility
        ce_sl_before_round = index_price + self.volatility
        ce_strike = self._round_strike(ce_sl_before_round)
        
        ce_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": "CE",
            "strike": ce_strike,
            "entry_index_price": index_price,
            "sl_index": ce_sl_before_round,
            "sl_before_round": ce_sl_before_round,
            "volatility": self.volatility
        }
        
        self.legs.append(ce_leg)
        
        actions.append({
            "action": "ENTER",
            "leg_id": ce_leg["leg_id"],
            "type": "CE",
            "strike": ce_strike,
            "entry_index_price": index_price,
            "sl_index": ce_sl_before_round,
            "sl_before_round": ce_sl_before_round,
            "volatility": self.volatility,
            "upper": ce_sl_before_round,  # For UPPER_RANGE column
            "lower": None,
            "R": self.volatility
        })
        
        # PE leg: index_price - volatility
        pe_sl_before_round = index_price - self.volatility
        pe_strike = self._round_strike(pe_sl_before_round)
        
        pe_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": "PE",
            "strike": pe_strike,
            "entry_index_price": index_price,
            "sl_index": pe_sl_before_round,
            "sl_before_round": pe_sl_before_round,
            "volatility": self.volatility
        }
        
        self.legs.append(pe_leg)
        
        actions.append({
            "action": "ENTER",
            "leg_id": pe_leg["leg_id"],
            "type": "PE",
            "strike": pe_strike,
            "entry_index_price": index_price,
            "sl_index": pe_sl_before_round,
            "sl_before_round": pe_sl_before_round,
            "volatility": self.volatility,
            "upper": None,
            "lower": pe_sl_before_round,  # For LOWER_RANGE column
            "R": self.volatility
        })
        
        return actions

    def _create_new_leg(self, old_leg, current_index_price):
        """Create new leg after SL hit"""
        actions = []
        
        opt_type = old_leg["type"]
        
        if opt_type == "CE":
            # New CE: old_sl + volatility
            new_sl_before_round = old_leg["sl_index"] + self.volatility
            new_strike = self._round_strike(new_sl_before_round)
            
        else:  # PE
            # New PE: old_sl - volatility
            new_sl_before_round = old_leg["sl_index"] - self.volatility
            new_strike = self._round_strike(new_sl_before_round)
        
        new_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": opt_type,
            "strike": new_strike,
            "entry_index_price": current_index_price,
            "sl_index": new_sl_before_round,
            "sl_before_round": new_sl_before_round,
            "volatility": self.volatility
        }
        
        self.legs.append(new_leg)
        
        actions.append({
            "action": "ENTER",
            "leg_id": new_leg["leg_id"],
            "type": opt_type,
            "strike": new_strike,
            "entry_index_price": current_index_price,
            "sl_index": new_sl_before_round,
            "sl_before_round": new_sl_before_round,
            "volatility": self.volatility,
            "upper": new_sl_before_round if opt_type == "CE" else None,
            "lower": new_sl_before_round if opt_type == "PE" else None,
            "R": self.volatility
        })
        
        return actions

    def _exit_leg(self, leg, reason):
        """Create exit action for a leg"""
        return {
            "action": "EXIT",
            "leg_id": leg["leg_id"],
            "reason": reason
        }

    def _round_strike(self, price):
        """Round price to nearest strike"""
        return round(price / self.STRIKE_GAP) * self.STRIKE_GAP