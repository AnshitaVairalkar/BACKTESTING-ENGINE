from datetime import time
from strategy.base_strategy import BaseStrategy
import itertools
import pandas as pd


class VolatilityStraddles(BaseStrategy):
    """
    Volatility-based Straddle Strategy (ATM re-entry)
    
    Logic:
    1. Enter ATM CE + ATM PE at 9:20
    2. CE SL = Entry Index + Range, PE SL = Entry Index - Range
    3. If CE breached: Exit CE, Enter NEW ATM CE (from breach index price)
    4. If PE breached: Exit PE, Enter NEW ATM PE (from breach index price)
    5. Remaining leg keeps original SL
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
        """Load volatility data from CSV"""
        df = pd.read_csv(self.volatility_csv_path)
        df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y").dt.strftime("%Y-%m-%d")
        self.volatility_map = dict(zip(df["Date"], df["CalculatedVolatility"]))

    # Required by BaseStrategy
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
        
        self.legs = []
        self.leg_counter = itertools.count(1)
        self.initial_index_open = None
        
        self.volatility = self._get_volatility_for_date(trade_date)
        
        if self.volatility is None:
            raise ValueError(f"No volatility data for {trade_date}")
        
        print(f"  📊 Volatility for {trade_date}: {self.volatility:.2f}")

    def _get_volatility_for_date(self, trade_date):
        """Get calculated volatility for the trade date"""
        if trade_date in self.volatility_map:
            return self.volatility_map[trade_date]
        
        dates = sorted([d for d in self.volatility_map.keys() if d < trade_date])
        if dates:
            return self.volatility_map[dates[-1]]
        
        return None

    def on_minute(self, ts, index_price):
        """Process each minute's index price"""
        actions = []
        candle_time = ts.time()

        # ================== INITIAL ENTRY ==================
        if not self.legs and candle_time >= self.ENTRY_TIME:
            if self.initial_index_open is None:
                self.initial_index_open = index_price
            actions += self._create_initial_straddle(self.initial_index_open)
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
                actions.append(self._exit_leg(leg, breach_reason))
                exited_legs.append(leg)
                
                # Re-enter ATM for breached leg only
                actions += self._create_new_atm_leg(leg["type"], index_price)

        for leg in exited_legs:
            self.legs.remove(leg)

        return actions

    def on_day_end(self):
        """Cleanup at end of day"""
        pass

    # =================================================
    # INTERNAL HELPERS
    # =================================================

    def _create_initial_straddle(self, index_price):
        """Create initial ATM CE + ATM PE at 9:20"""
        actions = []
        
        # ATM strike (same for both CE and PE)
        atm_strike = self._round_strike(index_price)
        
        # CE leg: SL = index + range
        ce_sl = index_price + self.volatility
        
        ce_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": "CE",
            "strike": atm_strike,
            "entry_index_price": index_price,
            "sl_index": ce_sl,
            "sl_before_round": ce_sl,
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
            "sl_before_round": ce_sl,
            "volatility": self.volatility,
            "upper": ce_sl,
            "lower": None
        })
        
        # PE leg: SL = index - range
        pe_sl = index_price - self.volatility
        
        pe_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": "PE",
            "strike": atm_strike,
            "entry_index_price": index_price,
            "sl_index": pe_sl,
            "sl_before_round": pe_sl,
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
            "sl_before_round": pe_sl,
            "volatility": self.volatility,
            "upper": None,
            "lower": pe_sl
        })
        
        return actions

    def _create_new_atm_leg(self, opt_type, current_index_price):
        """Create new ATM leg after SL breach (only for breached side)"""
        actions = []
        
        # New ATM strike based on current index price
        new_atm_strike = self._round_strike(current_index_price)
        
        if opt_type == "CE":
            # New CE: SL = current_index + range
            new_sl = current_index_price + self.volatility
        else:  # PE
            # New PE: SL = current_index - range
            new_sl = current_index_price - self.volatility
        
        new_leg = {
            "leg_id": f"L{next(self.leg_counter)}",
            "type": opt_type,
            "strike": new_atm_strike,
            "entry_index_price": current_index_price,
            "sl_index": new_sl,
            "sl_before_round": new_sl,
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
            "sl_before_round": new_sl,
            "volatility": self.volatility,
            "upper": new_sl if opt_type == "CE" else None,
            "lower": new_sl if opt_type == "PE" else None
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