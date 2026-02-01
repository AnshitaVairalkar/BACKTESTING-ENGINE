from datetime import time
from strategy.base_strategy import BaseStrategy


class ITMStraddle(BaseStrategy):
    """
    ITM Straddle Strategy
    
    Sell ITM Call (below ATM) and ITM Put (above ATM)
    Entry: 09:20 | Exit: 15:15 | SL: 40%
    """
    
    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 15)
    SL_PCT = 0.40
    STRIKE_GAP = 100

    def get_strikes(self, spot_price: float) -> dict:
        """Calculate ITM strike prices"""
        atm = round(spot_price / self.STRIKE_GAP) * self.STRIKE_GAP
        return {
            "CE": atm - self.STRIKE_GAP,  # ITM Call (below ATM)
            "PE": atm + self.STRIKE_GAP   # ITM Put (above ATM)
        }
    
    def get_leg_qty(self, leg_id: str) -> int:
        """Return -1 for both legs (sell straddle)"""
        return -1