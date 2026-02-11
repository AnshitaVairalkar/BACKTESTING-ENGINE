"""
Monte Carlo Analysis - Bootstrap + Parameter Sensitivity
Simple approach: Two separate analyses, save to single CSV
"""

import pandas as pd
import numpy as np
from pathlib import Path


class MonteCarloAnalysis:
    """
    Monte Carlo analysis for options strategies
    - Bootstrap: Resample actual trades
    - Parameter Sensitivity: Test with varied volatility
    """
    
    def __init__(self, trades_csv_path: str):
        self.trades_csv_path = trades_csv_path
        self.df = pd.read_csv(trades_csv_path)
        self.validate_data()
        
    def validate_data(self):
        """Check required columns"""
        required = ['DATE', 'PNL']
        missing = [col for col in required if col not in self.df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
    
    # ========================================================================
    # BOOTSTRAP ANALYSIS (Same as before)
    # ========================================================================
    
    def run_bootstrap(self, num_simulations: int = 10000, seed: int = 42):
        """
        Bootstrap: Resample actual trading days
        
        What it does:
        - Takes your 100 actual trading days
        - Randomly picks 100 days WITH replacement (same day can appear multiple times)
        - Calculates total PnL, max DD, win rate for this sample
        - Repeats 10,000 times
        - Shows you distribution of possible outcomes
        """
        np.random.seed(seed)
        
        # Group trades by day
        daily_pnl = self.df.groupby('DATE')['PNL'].sum().values
        n_days = len(daily_pnl)
        
        print(f"\n📊 BOOTSTRAP ANALYSIS")
        print(f"   Trading days: {n_days}")
        print(f"   Simulations: {num_simulations:,}")
        
        results = []
        
        for sim in range(num_simulations):
            # Randomly sample days
            sampled_indices = np.random.choice(n_days, size=n_days, replace=True)
            sampled_daily_pnl = daily_pnl[sampled_indices]
            
            # Calculate metrics
            total_pnl = sampled_daily_pnl.sum()
            cumulative = np.cumsum(sampled_daily_pnl)
            running_max = np.maximum.accumulate(cumulative)
            drawdown = cumulative - running_max
            max_dd = abs(drawdown.min())
            
            win_rate = (sampled_daily_pnl > 0).mean()
            wins = sampled_daily_pnl[sampled_daily_pnl > 0]
            losses = sampled_daily_pnl[sampled_daily_pnl < 0]
            
            avg_win = wins.mean() if len(wins) > 0 else 0
            avg_loss = losses.mean() if len(losses) > 0 else 0
            profit_factor = wins.sum() / abs(losses.sum()) if len(losses) > 0 else np.inf
            
            results.append({
                'simulation': sim,
                'analysis_type': 'bootstrap',
                'volatility_multiplier': 1.0,
                'total_pnl': total_pnl,
                'max_drawdown': max_dd,
                'win_rate': win_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor
            })
            
            if (sim + 1) % 2000 == 0:
                print(f"   Progress: {sim + 1:,}/{num_simulations:,}")
        
        return pd.DataFrame(results)
    
    # ========================================================================
    # PARAMETER SENSITIVITY ANALYSIS (NEW)
    # ========================================================================
    
    def run_parameter_sensitivity(self, num_simulations: int = 1000, 
                                  volatility_range: tuple = (0.8, 1.2), seed: int = 43):
        """
        Parameter Sensitivity: Test with different volatility values
        
        What it does:
        - Your strategy uses volatility to set SL ranges
        - This tests: "What if volatility was 10% higher or lower?"
        - Randomly varies volatility between 80% to 120% of actual
        - Simulates impact on PnL
        
        Logic:
        - If volatility ↑ 20% → Wider SLs → More losses → PnL ↓
        - If volatility ↓ 20% → Tighter SLs → Better entries → PnL ↑
        - Uses INVERSE relationship: PnL = Original PnL / Volatility_Multiplier
        
        Example:
        - Original vol = 100, PnL = 1000
        - Test with vol = 120 (1.2x) → PnL = 1000/1.2 = 833 (worse)
        - Test with vol = 80 (0.8x) → PnL = 1000/0.8 = 1250 (better)
        """
        np.random.seed(seed)
        
        # Check if strategy uses volatility
        if 'VOLATILITY' not in self.df.columns and 'RANGE_USED' not in self.df.columns:
            print("⚠️  Strategy doesn't use volatility - skipping sensitivity analysis")
            return None
        
        print(f"\n🔧 PARAMETER SENSITIVITY ANALYSIS")
        print(f"   Volatility range: {volatility_range[0]*100:.0f}% to {volatility_range[1]*100:.0f}%")
        print(f"   Simulations: {num_simulations:,}")
        print(f"   Logic: PnL inversely proportional to volatility")
        
        results = []
        
        for sim in range(num_simulations):
            # Random volatility multiplier (e.g., 0.85, 0.93, 1.12, etc.)
            vol_multiplier = np.random.uniform(volatility_range[0], volatility_range[1])
            
            # Adjust PnL based on volatility change
            # Inverse relationship: higher vol = worse PnL
            pnl_adjustment = 1.0 / vol_multiplier
            adjusted_pnl = self.df['PNL'].values * pnl_adjustment
            
            # Group by date
            daily_pnl = pd.DataFrame({
                'DATE': self.df['DATE'],
                'PNL': adjusted_pnl
            }).groupby('DATE')['PNL'].sum().values
            
            # Calculate metrics
            total_pnl = daily_pnl.sum()
            cumulative = np.cumsum(daily_pnl)
            running_max = np.maximum.accumulate(cumulative)
            drawdown = cumulative - running_max
            max_dd = abs(drawdown.min())
            
            win_rate = (daily_pnl > 0).mean()
            
            results.append({
                'simulation': sim,
                'analysis_type': 'parameter_sensitivity',
                'volatility_multiplier': vol_multiplier,
                'total_pnl': total_pnl,
                'max_drawdown': max_dd,
                'win_rate': win_rate,
                'avg_win': None,  # Not calculated for sensitivity
                'avg_loss': None,
                'profit_factor': None
            })
            
            if (sim + 1) % 200 == 0:
                print(f"   Progress: {sim + 1:,}/{num_simulations:,}")
        
        return pd.DataFrame(results)
    
    # ========================================================================
    # COMBINED RUN
    # ========================================================================
    
    def run_both(self, bootstrap_sims: int = 10000, sensitivity_sims: int = 1000,
                 volatility_range: tuple = (0.8, 1.2)):
        """
        Run both analyses and combine into single DataFrame
        """
        
        print("\n" + "="*70)
        print("MONTE CARLO ANALYSIS - BOOTSTRAP + PARAMETER SENSITIVITY")
        print("="*70)
        
        # 1. Bootstrap
        bootstrap_results = self.run_bootstrap(num_simulations=bootstrap_sims)
        
        # 2. Parameter Sensitivity
        sensitivity_results = self.run_parameter_sensitivity(
            num_simulations=sensitivity_sims,
            volatility_range=volatility_range
        )
        
        # 3. Combine
        if sensitivity_results is not None:
            combined = pd.concat([bootstrap_results, sensitivity_results], ignore_index=True)
            print(f"\n✅ Combined: {len(combined):,} total simulations")
        else:
            combined = bootstrap_results
            print(f"\n✅ Bootstrap only: {len(combined):,} simulations")
        
        return combined
    
    def print_summary(self, combined_results: pd.DataFrame):
        """Print summary statistics"""
        
        bootstrap = combined_results[combined_results['analysis_type'] == 'bootstrap']
        sensitivity = combined_results[combined_results['analysis_type'] == 'parameter_sensitivity']
        
        print("\n" + "="*70)
        print("RESULTS SUMMARY")
        print("="*70)
        
        # Bootstrap
        print(f"\n📊 BOOTSTRAP ({len(bootstrap):,} simulations)")
        print(f"   Mean PnL:      {bootstrap['total_pnl'].mean():>12,.2f}")
        print(f"   Std Dev:       {bootstrap['total_pnl'].std():>12,.2f}")
        print(f"   95% CI:        [{bootstrap['total_pnl'].quantile(0.025):>10,.2f}, {bootstrap['total_pnl'].quantile(0.975):>10,.2f}]")
        print(f"   P(Loss):       {(bootstrap['total_pnl'] < 0).mean()*100:>6.1f}%")
        print(f"   Max Drawdown:  {bootstrap['max_drawdown'].mean():>12,.2f}")
        print(f"   Win Rate:      {bootstrap['win_rate'].mean()*100:>6.1f}%")
        
        # VaR and Expected Shortfall
        print(f"\n📉 RISK METRICS (Bootstrap)")
        
        # VaR at different confidence levels
        var_95 = -bootstrap['total_pnl'].quantile(0.05)  # 5th percentile (negative = loss)
        var_99 = -bootstrap['total_pnl'].quantile(0.01)  # 1st percentile
        
        print(f"   VaR (95%):     {var_95:>12,.2f}  (5% chance of losing MORE than this)")
        print(f"   VaR (99%):     {var_99:>12,.2f}  (1% chance of losing MORE than this)")
        
        # Expected Shortfall (CVaR) - average loss beyond VaR
        tail_5 = bootstrap['total_pnl'][bootstrap['total_pnl'] <= bootstrap['total_pnl'].quantile(0.05)]
        tail_1 = bootstrap['total_pnl'][bootstrap['total_pnl'] <= bootstrap['total_pnl'].quantile(0.01)]
        
        es_95 = -tail_5.mean() if len(tail_5) > 0 else 0
        es_99 = -tail_1.mean() if len(tail_1) > 0 else 0
        
        print(f"   ES/CVaR (95%): {es_95:>12,.2f}  (avg loss in worst 5% scenarios)")
        print(f"   ES/CVaR (99%): {es_99:>12,.2f}  (avg loss in worst 1% scenarios)")
        
        # Interpretation
        if var_95 < 0:
            print(f"\n   ✅ VaR is NEGATIVE = No downside risk at 95% level")
            print(f"      Even worst 5% scenarios are profitable!")
        else:
            print(f"\n   ⚠️  Risk: 5% chance of losing ₹{var_95:,.0f} or more")
        
        # Parameter Sensitivity
        if len(sensitivity) > 0:
            print(f"\n🔧 PARAMETER SENSITIVITY ({len(sensitivity):,} simulations)")
            print(f"   Vol Range:     {sensitivity['volatility_multiplier'].min():.2f}x to {sensitivity['volatility_multiplier'].max():.2f}x")
            print(f"   Mean PnL:      {sensitivity['total_pnl'].mean():>12,.2f}")
            print(f"   Std Dev:       {sensitivity['total_pnl'].std():>12,.2f}")
            print(f"   95% CI:        [{sensitivity['total_pnl'].quantile(0.025):>10,.2f}, {sensitivity['total_pnl'].quantile(0.975):>10,.2f}]")
            print(f"   P(Loss):       {(sensitivity['total_pnl'] < 0).mean()*100:>6.1f}%")
            
            # VaR for sensitivity
            var_sens_95 = -sensitivity['total_pnl'].quantile(0.05)
            var_sens_99 = -sensitivity['total_pnl'].quantile(0.01)
            
            print(f"\n   VaR (95%):     {var_sens_95:>12,.2f}")
            print(f"   VaR (99%):     {var_sens_99:>12,.2f}")
            
            # Correlation
            corr = sensitivity[['volatility_multiplier', 'total_pnl']].corr().iloc[0, 1]
            print(f"   Vol-PnL Corr:  {corr:>6.2f} {'✅ Inverse (expected)' if corr < 0 else '⚠️  Positive (unexpected)'}")
            
            # Show impact
            base_pnl = bootstrap['total_pnl'].mean()
            low_vol_pnl = sensitivity[sensitivity['volatility_multiplier'] < 0.9]['total_pnl'].mean()
            high_vol_pnl = sensitivity[sensitivity['volatility_multiplier'] > 1.1]['total_pnl'].mean()
            
            print(f"\n   💡 VOLATILITY IMPACT:")
            print(f"      Base PnL (1.0x vol):  {base_pnl:>10,.2f}")
            print(f"      Low Vol (<0.9x):      {low_vol_pnl:>10,.2f}  ({(low_vol_pnl/base_pnl-1)*100:+.1f}%)")
            print(f"      High Vol (>1.1x):     {high_vol_pnl:>10,.2f}  ({(high_vol_pnl/base_pnl-1)*100:+.1f}%)")
        
        print("\n" + "="*70)
    
    def save_results(self, combined_results: pd.DataFrame, output_path: str):
        """Save to CSV"""
        combined_results.to_csv(output_path, index=False)
        print(f"\n💾 Results saved: {output_path}")
        print(f"   Columns: {', '.join(combined_results.columns)}")


def main():
    """Example usage"""
    
    # Input
    TRADES_CSV = "output/nifty_dynamicatminventory_20210601_20251231.csv"
    
    # Initialize
    mc = MonteCarloAnalysis(TRADES_CSV)
    
    # Run both analyses
    results = mc.run_both(
        bootstrap_sims=10000,       # Bootstrap simulations
        sensitivity_sims=10000,      # Parameter sensitivity simulations
        volatility_range=(0.8, 1.2) # Test volatility ±20%
    )
    
    # Print summary
    mc.print_summary(results)
    
    # Save
    mc.save_results(results, "output/monte_carlo_results.csv")
    
    print("\n✅ Done!")


if __name__ == "__main__":
    main()