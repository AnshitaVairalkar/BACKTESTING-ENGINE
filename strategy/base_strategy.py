from abc import ABC, abstractmethod
from datetime import time
from typing import Dict


class BaseStrategy(ABC):
    """
    Base class for all option strategies.
    
    To create a new strategy:
    1. Inherit from this class
    2. Set ENTRY_TIME, EXIT_TIME, SL_PCT, STRIKE_GAP
    3. Implement get_strikes() method
    4. Implement get_leg_qty() method
    """
    
    # Strategy parameters (override in subclass)
    ENTRY_TIME = time(9, 20)
    EXIT_TIME = time(15, 15)
    SL_PCT = 0.40
    STRIKE_GAP = 100
    
    @abstractmethod
    def get_strikes(self, spot_price: float) -> Dict[str, int]:
        """
        Calculate strike prices for all legs.
        
        Args:
            spot_price: Current spot price of the index
            
        Returns:
            Dictionary mapping leg_id to strike price
            Example: {"CE": 76400, "PE": 76600}
        """
        pass
    
    @abstractmethod
    def get_leg_qty(self, leg_id: str) -> int:
        """
        Get quantity for a specific leg.
        
        Args:
            leg_id: Leg identifier (e.g., 'CE', 'PE', 'CE_BUY', 'PE_SELL')
            
        Returns:
            -1 for sell/short, +1 for buy/long
        """
        pass
    
    def get_strategy_name(self) -> str:
        """Return the strategy name"""
        return self.__class__.__name__