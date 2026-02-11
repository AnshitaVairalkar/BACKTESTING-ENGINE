    # """
    # Strategy Analytics - Comprehensive Performance Metrics
    # Calculates and appends strategy performance to strategy_summary.csv
    # """

    # import pandas as pd
    # import numpy as np
    # import os
    # from pathlib import Path
    # from datetime import datetime


    # class StrategyAnalytics:
    #     """
    #     Calculate comprehensive strategy performance metrics
    #     """
        
    #     def __init__(self, trades_csv_path: str, margin: float = 100000, lot_size: int = 1):
    #         """
    #         Args:
    #             trades_csv_path: Path to backtest results CSV
    #             margin: Capital/Margin allocated to strategy (default: 1L)
    #             lot_size: Lot size multiplier for PnL (default: 1)
    #         """
    #         self.trades_csv_path = trades_csv_path
    #         self.margin = margin
    #         self.lot_size = lot_size
    #         self.df = pd.read_csv(trades_csv_path)
    #         self.strategy_name = self._extract_strategy_name()
            
    #         # Validate required columns
    #         self._validate_data()
            
    #         # Scale PnL by lot size
    #         self.df['PNL'] = self.df['PNL'] * lot_size
            
    #         # Prepare daily aggregations
    #         self.daily_pnl = self.df.groupby('DATE')['PNL'].sum()
    #         self.daily_pnl.index = pd.to_datetime(self.daily_pnl.index)
        
    #     def _validate_data(self):
    #         """Check required columns exist"""
    #         required = ['DATE', 'PNL', 'TYPE', 'EXIT_REASON']
    #         missing = [col for col in required if col not in self.df.columns]
    #         if missing:
    #             raise ValueError(f"Missing columns: {missing}")
        
    #     def _extract_strategy_name(self):
    #         """Extract strategy name from filename"""
    #         filename = Path(self.trades_csv_path).stem
    #         parts = filename.split('_')
    #         if len(parts) >= 2:
    #             return parts[1].title()
    #         return filename
        
    #     def calculate_all_metrics(self):
    #         """Calculate all performance metrics"""
            
    #         metrics = {}
            
    #         # Basic info
    #         metrics['STRATEGY'] = self.strategy_name
    #         metrics['LOT_SIZE'] = self.lot_size
    #         metrics['START_DATE'] = self.daily_pnl.index.min().strftime('%Y-%m-%d')
    #         metrics['END_DATE'] = self.daily_pnl.index.max().strftime('%Y-%m-%d')
    #         metrics['TOTAL_DAYS'] = len(self.daily_pnl)
    #         metrics['TOTAL_TRADES'] = len(self.df)
            
    #         # PnL metrics
    #         metrics['TOTAL_PNL'] = self.daily_pnl.sum()
    #         metrics['MONTHLY_AVG_PNL'] = self.daily_pnl.sum() / (len(self.daily_pnl) / 21)
    #         metrics['DAILY_AVG_PNL'] = self.daily_pnl.mean()
            
    #         # Win/Loss metrics
    #         winning_days = self.daily_pnl[self.daily_pnl > 0]
    #         losing_days = self.daily_pnl[self.daily_pnl < 0]
            
    #         metrics['WIN_RATE'] = len(winning_days) / len(self.daily_pnl) * 100
    #         metrics['NUM_WINNING_DAYS'] = len(winning_days)
    #         metrics['NUM_LOSING_DAYS'] = len(losing_days)
    #         metrics['AVG_WIN'] = winning_days.mean() if len(winning_days) > 0 else 0
    #         metrics['AVG_LOSS'] = losing_days.mean() if len(losing_days) > 0 else 0
    #         metrics['MAX_WIN'] = winning_days.max() if len(winning_days) > 0 else 0
    #         metrics['MAX_LOSS'] = losing_days.min() if len(losing_days) > 0 else 0
            
    #         # Profit factor
    #         total_wins = winning_days.sum() if len(winning_days) > 0 else 0
    #         total_losses = abs(losing_days.sum()) if len(losing_days) > 0 else 0
    #         metrics['PROFIT_FACTOR'] = total_wins / total_losses if total_losses > 0 else np.inf
            
    #         # Streaks
    #         metrics['MAX_WINNING_STREAK'] = self._calculate_max_streak(self.daily_pnl > 0)
    #         metrics['MAX_LOSING_STREAK'] = self._calculate_max_streak(self.daily_pnl < 0)
    #         metrics['CURRENT_STREAK'] = self._calculate_current_streak()
            
    #         # Drawdown metrics
    #         dd_metrics = self._calculate_drawdown_metrics()
    #         metrics.update(dd_metrics)
            
    #         # Risk-adjusted returns
    #         metrics['SHARPE_RATIO'] = self._calculate_sharpe_ratio()
    #         metrics['SORTINO_RATIO'] = self._calculate_sortino_ratio()
    #         metrics['CALMAR_RATIO'] = metrics['TOTAL_PNL'] / abs(dd_metrics['MAX_DRAWDOWN']) if dd_metrics['MAX_DRAWDOWN'] != 0 else np.inf
            
    #         # Trade-level metrics
    #         metrics['HIT_RATIO'] = (self.df['PNL'] > 0).sum() / len(self.df) * 100
    #         metrics['AVG_TRADE_PNL'] = self.df['PNL'].mean()
            
    #         # Expectancy
    #         win_prob = len(winning_days) / len(self.daily_pnl)
    #         loss_prob = len(losing_days) / len(self.daily_pnl)
    #         avg_win = winning_days.mean() if len(winning_days) > 0 else 0
    #         avg_loss = abs(losing_days.mean()) if len(losing_days) > 0 else 0
    #         metrics['EXPECTANCY'] = (win_prob * avg_win) - (loss_prob * avg_loss)
            
    #         # Additional metrics
    #         metrics['MARGIN'] = self.margin
    #         metrics['TOTAL_RETURN_PCT'] = (metrics['TOTAL_PNL'] / self.margin) * 100
    #         metrics['MONTHLY_RETURN_PCT'] = (metrics['MONTHLY_AVG_PNL'] / self.margin) * 100
    #         metrics['RECOVERY_FACTOR'] = metrics['TOTAL_PNL'] / abs(dd_metrics['MAX_DRAWDOWN']) if dd_metrics['MAX_DRAWDOWN'] != 0 else np.inf
            
    #         return metrics
        
    #     def _calculate_max_streak(self, condition):
    #         """Calculate maximum consecutive streak"""
    #         if len(condition) == 0:
    #             return 0
            
    #         streaks = []
    #         current_streak = 0
            
    #         for val in condition:
    #             if val:
    #                 current_streak += 1
    #             else:
    #                 if current_streak > 0:
    #                     streaks.append(current_streak)
    #                 current_streak = 0
            
    #         if current_streak > 0:
    #             streaks.append(current_streak)
            
    #         return max(streaks) if streaks else 0
        
    #     def _calculate_current_streak(self):
    #         """Calculate current winning/losing streak"""
    #         if len(self.daily_pnl) == 0:
    #             return 0
            
    #         streak = 0
    #         last_positive = self.daily_pnl.iloc[-1] > 0
            
    #         for pnl in reversed(self.daily_pnl.values):
    #             if (pnl > 0) == last_positive:
    #                 streak += 1
    #             else:
    #                 break
            
    #         return streak if last_positive else -streak
        
    #     def _calculate_drawdown_metrics(self):
    #         """Calculate drawdown-related metrics"""
    #         cumulative = self.daily_pnl.cumsum()
    #         running_max = cumulative.cummax()
    #         drawdown = cumulative - running_max  # Negative when in drawdown
            
    #         max_dd = abs(drawdown.min())  # Most negative = biggest DD
    #         max_dd_pct = (max_dd / running_max.max() * 100) if running_max.max() > 0 else 0
            
    #         max_dd_idx = drawdown.idxmin()  # When DD was deepest
            
    #         # Find when DD started (last peak before max DD)
    #         dd_start = running_max[:max_dd_idx].idxmax()
            
    #         # Find recovery point (when cumulative >= peak again)
    #         recovery_idx = cumulative[max_dd_idx:][cumulative[max_dd_idx:] >= running_max[max_dd_idx]]
            
    #         if len(recovery_idx) > 0:
    #             time_to_recover = (recovery_idx.index[0] - max_dd_idx).days
    #         else:
    #             time_to_recover = -1
            
    #         return {
    #             'MAX_DRAWDOWN': max_dd,
    #             'MAX_DRAWDOWN_PCT': max_dd_pct,
    #             'MAX_DRAWDOWN_DATE': max_dd_idx.strftime('%Y-%m-%d'),
    #             'TIME_TO_RECOVER_DAYS': time_to_recover,
    #             'AVG_DRAWDOWN': abs(drawdown[drawdown < 0].mean()) if (drawdown < 0).any() else 0,
    #             'NUM_DRAWDOWN_PERIODS': (drawdown < 0).sum()
    #         }
        
    #     def _calculate_sharpe_ratio(self, risk_free_rate=0.06):
    #         """Calculate Sharpe ratio"""
    #         daily_returns = self.daily_pnl
            
    #         if len(daily_returns) < 2:
    #             return 0
            
    #         excess_returns = daily_returns - (risk_free_rate / 252)
            
    #         if daily_returns.std() == 0:
    #             return 0
            
    #         sharpe = (excess_returns.mean() / daily_returns.std()) * np.sqrt(252)
    #         return sharpe
        
    #     def _calculate_sortino_ratio(self, risk_free_rate=0.06):
    #         """Calculate Sortino ratio"""
    #         daily_returns = self.daily_pnl
            
    #         if len(daily_returns) < 2:
    #             return 0
            
    #         excess_returns = daily_returns - (risk_free_rate / 252)
    #         downside_returns = daily_returns[daily_returns < 0]
            
    #         if len(downside_returns) == 0:
    #             return np.inf
            
    #         downside_std = downside_returns.std()
            
    #         if downside_std == 0:
    #             return 0
            
    #         sortino = (excess_returns.mean() / downside_std) * np.sqrt(252)
    #         return sortino
        
    #     def print_summary(self, metrics):
    #         """Print formatted summary"""
            
    #         print("\n" + "="*80)
    #         print(f"STRATEGY PERFORMANCE SUMMARY: {metrics['STRATEGY']}")
    #         print("="*80)
            
    #         print(f"\n📅 PERIOD")
    #         print(f"   Lot Size:     {metrics['LOT_SIZE']}")
    #         print(f"   Start:        {metrics['START_DATE']}")
    #         print(f"   End:          {metrics['END_DATE']}")
    #         print(f"   Trading Days: {metrics['TOTAL_DAYS']}")
    #         print(f"   Total Trades: {metrics['TOTAL_TRADES']}")
            
    #         print(f"\n💰 PnL METRICS")
    #         print(f"   Margin/Capital:  {metrics['MARGIN']:>12,.2f}")
    #         print(f"   Total PnL:       {metrics['TOTAL_PNL']:>12,.2f}")
    #         print(f"   Total Return %:  {metrics['TOTAL_RETURN_PCT']:>12.2f}%")
    #         print(f"   Monthly Avg:     {metrics['MONTHLY_AVG_PNL']:>12,.2f}")
    #         print(f"   Monthly Ret %:   {metrics['MONTHLY_RETURN_PCT']:>12.2f}%")
    #         print(f"   Daily Avg:       {metrics['DAILY_AVG_PNL']:>12,.2f}")
    #         print(f"   Max Win:         {metrics['MAX_WIN']:>12,.2f}")
    #         print(f"   Max Loss:        {metrics['MAX_LOSS']:>12,.2f}")
            
    #         print(f"\n🎯 WIN/LOSS")
    #         print(f"   Win Rate:        {metrics['WIN_RATE']:>6.1f}%")
    #         print(f"   Winning Days:    {metrics['NUM_WINNING_DAYS']:>6}")
    #         print(f"   Losing Days:     {metrics['NUM_LOSING_DAYS']:>6}")
    #         print(f"   Avg Win:         {metrics['AVG_WIN']:>12,.2f}")
    #         print(f"   Avg Loss:        {metrics['AVG_LOSS']:>12,.2f}")
    #         print(f"   Profit Factor:   {metrics['PROFIT_FACTOR']:>12.2f}")
            
    #         print(f"\n📊 STREAKS")
    #         print(f"   Max Winning:     {metrics['MAX_WINNING_STREAK']:>6} days")
    #         print(f"   Max Losing:      {metrics['MAX_LOSING_STREAK']:>6} days")
    #         print(f"   Current:         {metrics['CURRENT_STREAK']:>6} days")
            
    #         print(f"\n📉 DRAWDOWN")
    #         print(f"   Max Drawdown:    {metrics['MAX_DRAWDOWN']:>12,.2f} ({metrics['MAX_DRAWDOWN_PCT']:.1f}%)")
    #         print(f"   DD Date:         {metrics['MAX_DRAWDOWN_DATE']}")
    #         print(f"   Time to Recover: {metrics['TIME_TO_RECOVER_DAYS']:>6} days")
    #         print(f"   Avg Drawdown:    {metrics['AVG_DRAWDOWN']:>12,.2f}")
            
    #         print(f"\n📈 RISK-ADJUSTED RETURNS")
    #         print(f"   Sharpe Ratio:    {metrics['SHARPE_RATIO']:>12.3f}")
    #         print(f"   Sortino Ratio:   {metrics['SORTINO_RATIO']:>12.3f}")
    #         print(f"   Calmar Ratio:    {metrics['CALMAR_RATIO']:>12.3f}")
    #         print(f"   Recovery Factor: {metrics['RECOVERY_FACTOR']:>12.3f}")
            
    #         print(f"\n🎲 TRADE METRICS")
    #         print(f"   Hit Ratio:       {metrics['HIT_RATIO']:>6.1f}%")
    #         print(f"   Avg Trade PnL:   {metrics['AVG_TRADE_PNL']:>12,.2f}")
    #         print(f"   Expectancy:      {metrics['EXPECTANCY']:>12,.2f}")
            
    #         print("\n" + "="*80)
        
    #     def append_to_summary(self, metrics):
    #         """Append metrics to strategy summary CSV"""
            
    #         metrics_df = pd.DataFrame([metrics])
    #         numeric_cols = metrics_df.select_dtypes(include=[np.number]).columns
    #         metrics_df[numeric_cols] = metrics_df[numeric_cols].round(2)
            
    #         script_dir = os.path.dirname(os.path.abspath(__file__))
    #         summary_path = Path(script_dir) / "strategy_summary.csv"
            
    #         if summary_path.exists():
    #             try:
    #                 existing = pd.read_csv(summary_path)
    #                 mask = (
    #                     (existing['STRATEGY'] == metrics['STRATEGY']) &
    #                     (existing['START_DATE'] == metrics['START_DATE']) &
    #                     (existing['END_DATE'] == metrics['END_DATE'])
    #                 )
                    
    #                 if mask.any():
    #                     print(f"\n⚠️  Updating existing entry for {metrics['STRATEGY']}")
    #                     existing.loc[mask] = metrics_df.values[0]
    #                     existing.to_csv(summary_path, index=False)
    #                 else:
    #                     print(f"\n✅ Appending new entry for {metrics['STRATEGY']}")
    #                     metrics_df.to_csv(summary_path, mode='a', header=False, index=False)
    #             except:
    #                 print(f"✅ Creating fresh file")
    #                 metrics_df.to_csv(summary_path, index=False)
    #         else:
    #             print(f"\n✅ Creating new summary file")
    #             metrics_df.to_csv(summary_path, index=False)
            
    #         print(f"💾 Summary saved: {summary_path}")


    # def analyze_strategy(trades_csv_path: str, margin: float = 100000, lot_size: int = 1, print_report=True, save_to_summary=True):
    #     """Main function to analyze a strategy
        
    #     Args:
    #         trades_csv_path: Path to backtest results CSV
    #         margin: Capital allocated to strategy
    #         lot_size: Lot size multiplier (e.g., 25 for NIFTY, 10 for BANKNIFTY)
    #         print_report: Whether to print summary report
    #         save_to_summary: Whether to append to strategy_summary.csv
    #     """
        
    #     print(f"\n📊 Analyzing: {trades_csv_path}")
        
    #     analytics = StrategyAnalytics(trades_csv_path, margin=margin, lot_size=lot_size)
    #     metrics = analytics.calculate_all_metrics()
        
    #     if print_report:
    #         analytics.print_summary(metrics)
        
    #     if save_to_summary:
    #         analytics.append_to_summary(metrics)
        
    #     return metrics


    # def main():
    #     """Example usage"""
        
    #     script_dir = os.path.dirname(os.path.abspath(__file__))
        
    #     # Define strategies with their respective margins and lot sizes
    #     strategies = [
    #         {
    #             "path": os.path.join(script_dir, "..", "output", "nifty_volatilitystraddles_20220101_20251231.csv"),
    #             "margin": 100000,  # 1 Lakh
    #             "lot_size": 65     # NIFTY lot size
    #         },
    #         {
    #             "path": os.path.join(script_dir, "..", "output", "nifty_volatilitystrangles_20220101_20251231.csv"),
    #             "margin": 100000,  # 1 Lakh
    #             "lot_size": 65     # NIFTY lot size
    #         },
    #         {
    #             "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventory_20210601_20251231.csv"),
    #             "margin": 150000,  # 1.5 Lakh
    #             "lot_size": 65     # NIFTY lot size
    #         },
    #         {
    #             "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventory_20210601_20251231_100 Range.csv"),
    #             "margin": 150000,  # 1.5 Lakh
    #             "lot_size": 65     # NIFTY lot size
    #         },
    #                 {
    #             "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventorylatestlevelcheck_20210601_20251231.csv"),
    #             "margin": 150000,  # 1.5 Lakh
    #             "lot_size": 65     # NIFTY lot size
    #         }

    #     ]
        
    #     for strat in strategies:
    #         try:
    #             analyze_strategy(
    #                 strat["path"], 
    #                 margin=strat["margin"],
    #                 lot_size=strat["lot_size"],
    #                 print_report=False, 
    #                 save_to_summary=True
    #             )
    #         except Exception as e:
    #             print(f"\n❌ Error analyzing {os.path.basename(strat['path'])}: {e}")
    #             continue
        
    #     print("\n✅ All strategies analyzed and saved to strategy_summary.csv")


    # if __name__ == "__main__":
    #     main()


"""
Strategy Analytics - Comprehensive Performance Metrics
Calculates and appends strategy performance to strategy_summary.csv
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path
from datetime import datetime


class StrategyAnalytics:
    """
    Calculate comprehensive strategy performance metrics
    """
    
    def __init__(self, trades_csv_path: str, margin: float = 100000, lot_size: int = 1):
        """
        Args:
            trades_csv_path: Path to backtest results CSV
            margin: Capital/Margin allocated to strategy (default: 1L)
            lot_size: Lot size multiplier for PnL (default: 1)
        """
        self.trades_csv_path = trades_csv_path
        self.margin = margin
        self.lot_size = lot_size
        self.df = pd.read_csv(trades_csv_path)
        self.strategy_name = self._extract_strategy_name()
        
        # Validate required columns
        self._validate_data()
        
        # Scale PnL by lot size
        self.df['PNL'] = self.df['PNL'] * lot_size
        
        # Prepare daily aggregations
        self.daily_pnl = self.df.groupby('DATE')['PNL'].sum()
        self.daily_pnl.index = pd.to_datetime(self.daily_pnl.index)
    
    def _validate_data(self):
        """Check required columns exist"""
        required = ['DATE', 'PNL', 'TYPE', 'EXIT_REASON']
        missing = [col for col in required if col not in self.df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
    
    def _extract_strategy_name(self):
        """Extract strategy name from filename"""
        return Path(self.trades_csv_path).stem
    
    def calculate_all_metrics(self):
        """Calculate all performance metrics"""
        
        metrics = {}
        
        # Basic info
        metrics['STRATEGY'] = self.strategy_name
        metrics['LOT_SIZE'] = self.lot_size
        metrics['START_DATE'] = self.daily_pnl.index.min().strftime('%Y-%m-%d')
        metrics['END_DATE'] = self.daily_pnl.index.max().strftime('%Y-%m-%d')
        metrics['TOTAL_DAYS'] = len(self.daily_pnl)
        metrics['TOTAL_TRADES'] = len(self.df)
        
        # PnL metrics
        metrics['TOTAL_PNL'] = self.daily_pnl.sum()
        metrics['MONTHLY_AVG_PNL'] = self.daily_pnl.sum() / (len(self.daily_pnl) / 21)
        metrics['DAILY_AVG_PNL'] = self.daily_pnl.mean()
        
        # Win/Loss metrics
        winning_days = self.daily_pnl[self.daily_pnl > 0]
        losing_days = self.daily_pnl[self.daily_pnl < 0]
        
        metrics['WIN_RATE'] = len(winning_days) / len(self.daily_pnl) * 100
        metrics['NUM_WINNING_DAYS'] = len(winning_days)
        metrics['NUM_LOSING_DAYS'] = len(losing_days)
        metrics['AVG_WIN'] = winning_days.mean() if len(winning_days) > 0 else 0
        metrics['AVG_LOSS'] = losing_days.mean() if len(losing_days) > 0 else 0
        metrics['MAX_WIN'] = winning_days.max() if len(winning_days) > 0 else 0
        metrics['MAX_LOSS'] = losing_days.min() if len(losing_days) > 0 else 0
        
        # Profit factor
        total_wins = winning_days.sum() if len(winning_days) > 0 else 0
        total_losses = abs(losing_days.sum()) if len(losing_days) > 0 else 0
        metrics['PROFIT_FACTOR'] = total_wins / total_losses if total_losses > 0 else np.inf
        
        # Streaks
        metrics['MAX_WINNING_STREAK'] = self._calculate_max_streak(self.daily_pnl > 0)
        metrics['MAX_LOSING_STREAK'] = self._calculate_max_streak(self.daily_pnl < 0)
        metrics['CURRENT_STREAK'] = self._calculate_current_streak()
        
        # Drawdown metrics
        dd_metrics = self._calculate_drawdown_metrics()
        metrics.update(dd_metrics)
        
        # Risk-adjusted returns
        metrics['SHARPE_RATIO'] = self._calculate_sharpe_ratio()
        metrics['SORTINO_RATIO'] = self._calculate_sortino_ratio()
        metrics['CALMAR_RATIO'] = metrics['TOTAL_PNL'] / abs(dd_metrics['MAX_DRAWDOWN']) if dd_metrics['MAX_DRAWDOWN'] != 0 else np.inf
        
        # Trade-level metrics
        metrics['HIT_RATIO'] = (self.df['PNL'] > 0).sum() / len(self.df) * 100
        metrics['AVG_TRADE_PNL'] = self.df['PNL'].mean()
        
        # Expectancy
        win_prob = len(winning_days) / len(self.daily_pnl)
        loss_prob = len(losing_days) / len(self.daily_pnl)
        avg_win = winning_days.mean() if len(winning_days) > 0 else 0
        avg_loss = abs(losing_days.mean()) if len(losing_days) > 0 else 0
        metrics['EXPECTANCY'] = (win_prob * avg_win) - (loss_prob * avg_loss)
        
        # Additional metrics
        metrics['MARGIN'] = self.margin
        metrics['TOTAL_RETURN_PCT'] = (metrics['TOTAL_PNL'] / self.margin) * 100
        metrics['MONTHLY_RETURN_PCT'] = (metrics['MONTHLY_AVG_PNL'] / self.margin) * 100
        metrics['RECOVERY_FACTOR'] = metrics['TOTAL_PNL'] / abs(dd_metrics['MAX_DRAWDOWN']) if dd_metrics['MAX_DRAWDOWN'] != 0 else np.inf
        
        return metrics
    
    def _calculate_max_streak(self, condition):
        """Calculate maximum consecutive streak"""
        if len(condition) == 0:
            return 0
        
        streaks = []
        current_streak = 0
        
        for val in condition:
            if val:
                current_streak += 1
            else:
                if current_streak > 0:
                    streaks.append(current_streak)
                current_streak = 0
        
        if current_streak > 0:
            streaks.append(current_streak)
        
        return max(streaks) if streaks else 0
    
    def _calculate_current_streak(self):
        """Calculate current winning/losing streak"""
        if len(self.daily_pnl) == 0:
            return 0
        
        streak = 0
        last_positive = self.daily_pnl.iloc[-1] > 0
        
        for pnl in reversed(self.daily_pnl.values):
            if (pnl > 0) == last_positive:
                streak += 1
            else:
                break
        
        return streak if last_positive else -streak
    
    def _calculate_drawdown_metrics(self):
        """Calculate drawdown-related metrics"""
        cumulative = self.daily_pnl.cumsum()
        running_max = cumulative.cummax()
        drawdown = cumulative - running_max  # Negative when in drawdown
        
        max_dd = abs(drawdown.min())  # Most negative = biggest DD
        max_dd_pct = (max_dd / running_max.max() * 100) if running_max.max() > 0 else 0
        
        max_dd_idx = drawdown.idxmin()  # When DD was deepest
        
        # Find when DD started (last peak before max DD)
        dd_start = running_max[:max_dd_idx].idxmax()
        
        # Find recovery point (when cumulative >= peak again)
        recovery_idx = cumulative[max_dd_idx:][cumulative[max_dd_idx:] >= running_max[max_dd_idx]]
        
        if len(recovery_idx) > 0:
            time_to_recover = (recovery_idx.index[0] - max_dd_idx).days
        else:
            time_to_recover = -1
        
        return {
            'MAX_DRAWDOWN': max_dd,
            'MAX_DRAWDOWN_PCT': max_dd_pct,
            'MAX_DRAWDOWN_DATE': max_dd_idx.strftime('%Y-%m-%d'),
            'TIME_TO_RECOVER_DAYS': time_to_recover,
            'AVG_DRAWDOWN': abs(drawdown[drawdown < 0].mean()) if (drawdown < 0).any() else 0,
            'NUM_DRAWDOWN_PERIODS': (drawdown < 0).sum()
        }
    
    def _calculate_sharpe_ratio(self, risk_free_rate=0.06):
        """Calculate Sharpe ratio"""
        daily_returns = self.daily_pnl
        
        if len(daily_returns) < 2:
            return 0
        
        excess_returns = daily_returns - (risk_free_rate / 252)
        
        if daily_returns.std() == 0:
            return 0
        
        sharpe = (excess_returns.mean() / daily_returns.std()) * np.sqrt(252)
        return sharpe
    
    def _calculate_sortino_ratio(self, risk_free_rate=0.06):
        """Calculate Sortino ratio"""
        daily_returns = self.daily_pnl
        
        if len(daily_returns) < 2:
            return 0
        
        excess_returns = daily_returns - (risk_free_rate / 252)
        downside_returns = daily_returns[daily_returns < 0]
        
        if len(downside_returns) == 0:
            return np.inf
        
        downside_std = downside_returns.std()
        
        if downside_std == 0:
            return 0
        
        sortino = (excess_returns.mean() / downside_std) * np.sqrt(252)
        return sortino
    
    def print_summary(self, metrics):
        """Print formatted summary"""
        
        print("\n" + "="*80)
        print(f"STRATEGY PERFORMANCE SUMMARY: {metrics['STRATEGY']}")
        print("="*80)
        
        print(f"\n📅 PERIOD")
        print(f"   Lot Size:     {metrics['LOT_SIZE']}")
        print(f"   Start:        {metrics['START_DATE']}")
        print(f"   End:          {metrics['END_DATE']}")
        print(f"   Trading Days: {metrics['TOTAL_DAYS']}")
        print(f"   Total Trades: {metrics['TOTAL_TRADES']}")
        
        print(f"\n💰 PnL METRICS")
        print(f"   Margin/Capital:  {metrics['MARGIN']:>12,.2f}")
        print(f"   Total PnL:       {metrics['TOTAL_PNL']:>12,.2f}")
        print(f"   Total Return %:  {metrics['TOTAL_RETURN_PCT']:>12.2f}%")
        print(f"   Monthly Avg:     {metrics['MONTHLY_AVG_PNL']:>12,.2f}")
        print(f"   Monthly Ret %:   {metrics['MONTHLY_RETURN_PCT']:>12.2f}%")
        print(f"   Daily Avg:       {metrics['DAILY_AVG_PNL']:>12,.2f}")
        print(f"   Max Win:         {metrics['MAX_WIN']:>12,.2f}")
        print(f"   Max Loss:        {metrics['MAX_LOSS']:>12,.2f}")
        
        print(f"\n🎯 WIN/LOSS")
        print(f"   Win Rate:        {metrics['WIN_RATE']:>6.1f}%")
        print(f"   Winning Days:    {metrics['NUM_WINNING_DAYS']:>6}")
        print(f"   Losing Days:     {metrics['NUM_LOSING_DAYS']:>6}")
        print(f"   Avg Win:         {metrics['AVG_WIN']:>12,.2f}")
        print(f"   Avg Loss:        {metrics['AVG_LOSS']:>12,.2f}")
        print(f"   Profit Factor:   {metrics['PROFIT_FACTOR']:>12.2f}")
        
        print(f"\n📊 STREAKS")
        print(f"   Max Winning:     {metrics['MAX_WINNING_STREAK']:>6} days")
        print(f"   Max Losing:      {metrics['MAX_LOSING_STREAK']:>6} days")
        print(f"   Current:         {metrics['CURRENT_STREAK']:>6} days")
        
        print(f"\n📉 DRAWDOWN")
        print(f"   Max Drawdown:    {metrics['MAX_DRAWDOWN']:>12,.2f} ({metrics['MAX_DRAWDOWN_PCT']:.1f}%)")
        print(f"   DD Date:         {metrics['MAX_DRAWDOWN_DATE']}")
        print(f"   Time to Recover: {metrics['TIME_TO_RECOVER_DAYS']:>6} days")
        print(f"   Avg Drawdown:    {metrics['AVG_DRAWDOWN']:>12,.2f}")
        
        print(f"\n📈 RISK-ADJUSTED RETURNS")
        print(f"   Sharpe Ratio:    {metrics['SHARPE_RATIO']:>12.3f}")
        print(f"   Sortino Ratio:   {metrics['SORTINO_RATIO']:>12.3f}")
        print(f"   Calmar Ratio:    {metrics['CALMAR_RATIO']:>12.3f}")
        print(f"   Recovery Factor: {metrics['RECOVERY_FACTOR']:>12.3f}")
        
        print(f"\n🎲 TRADE METRICS")
        print(f"   Hit Ratio:       {metrics['HIT_RATIO']:>6.1f}%")
        print(f"   Avg Trade PnL:   {metrics['AVG_TRADE_PNL']:>12,.2f}")
        print(f"   Expectancy:      {metrics['EXPECTANCY']:>12,.2f}")
        
        print("\n" + "="*80)
    
    def append_to_summary(self, metrics):
        """Append metrics to strategy summary CSV"""
        
        metrics_df = pd.DataFrame([metrics])
        numeric_cols = metrics_df.select_dtypes(include=[np.number]).columns
        metrics_df[numeric_cols] = metrics_df[numeric_cols].round(2)
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        summary_path = Path(script_dir) / "strategy_summary.csv"
        
        if summary_path.exists():
            try:
                existing = pd.read_csv(summary_path)
                mask = (
                    (existing['STRATEGY'] == metrics['STRATEGY']) &
                    (existing['START_DATE'] == metrics['START_DATE']) &
                    (existing['END_DATE'] == metrics['END_DATE'])
                )
                
                if mask.any():
                    print(f"\n⚠️  Updating existing entry for {metrics['STRATEGY']}")
                    existing.loc[mask] = metrics_df.values[0]
                    existing.to_csv(summary_path, index=False)
                else:
                    print(f"\n✅ Appending new entry for {metrics['STRATEGY']}")
                    metrics_df.to_csv(summary_path, mode='a', header=False, index=False)
            except:
                print(f"✅ Creating fresh file")
                metrics_df.to_csv(summary_path, index=False)
        else:
            print(f"\n✅ Creating new summary file")
            metrics_df.to_csv(summary_path, index=False)
        
        print(f"💾 Summary saved: {summary_path}")


def analyze_strategy(trades_csv_path: str, margin: float = 100000, lot_size: int = 1, print_report=True, save_to_summary=True):
    """Main function to analyze a strategy
    
    Args:
        trades_csv_path: Path to backtest results CSV
        margin: Capital allocated to strategy
        lot_size: Lot size multiplier (e.g., 25 for NIFTY, 10 for BANKNIFTY)
        print_report: Whether to print summary report
        save_to_summary: Whether to append to strategy_summary.csv
    """
    
    print(f"\n📊 Analyzing: {trades_csv_path}")
    
    analytics = StrategyAnalytics(trades_csv_path, margin=margin, lot_size=lot_size)
    metrics = analytics.calculate_all_metrics()
    
    if print_report:
        analytics.print_summary(metrics)
    
    if save_to_summary:
        analytics.append_to_summary(metrics)
    
    return metrics


def main():
    """Example usage"""
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define strategies with their respective margins and lot sizes
    strategies = [
        {
            "path": os.path.join(script_dir, "..", "output", "nifty_volatilitystraddles_20220101_20251231.csv"),
            "margin": 100000,  # 1 Lakh
            "lot_size": 65     # NIFTY lot size
        },
        {
            "path": os.path.join(script_dir, "..", "output", "nifty_volatilitystrangles_20220101_20251231.csv"),
            "margin": 100000,  # 1 Lakh
            "lot_size": 65     # NIFTY lot size
        },
        {
            "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventory_20210601_20251231.csv"),
            "margin": 150000,  # 1.5 Lakh
            "lot_size": 65     # NIFTY lot size
        },
        {
            "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventory_20210601_20251231_100 Range.csv"),
            "margin": 150000,  # 1.5 Lakh
            "lot_size": 65     # NIFTY lot size
        },
                {
            "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventorylatestlevelcheck_20210601_20251231.csv"),
            "margin": 150000,  # 1.5 Lakh
            "lot_size": 65     # NIFTY lot size
        },
                {
            "path": os.path.join(script_dir, "..", "output", "nifty_dynamicatminventorylatestlevelcheck_20210601_20251231_100 Range.csv"),
            "margin": 150000,  # 1.5 Lakh
            "lot_size": 65     # NIFTY lot size
        }

    ]
    
    for strat in strategies:
        try:
            analyze_strategy(
                strat["path"], 
                margin=strat["margin"],
                lot_size=strat["lot_size"],
                print_report=False, 
                save_to_summary=True
            )
        except Exception as e:
            print(f"\n❌ Error analyzing {os.path.basename(strat['path'])}: {e}")
            continue
    
    print("\n✅ All strategies analyzed and saved to strategy_summary.csv")


if __name__ == "__main__":
    main()