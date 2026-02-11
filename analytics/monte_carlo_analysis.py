"""
Monte Carlo Bootstrap Analysis for Options Strategies
Analyzes existing backtest results without modifying any backtest code
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt


class MonteCarloAnalysis:
    """
    Day-level bootstrap analysis for options strategies
    Resamples complete trading days to preserve CE+PE relationships
    """
    
    def __init__(self, trades_csv_path: str):
        """
        Args:
            trades_csv_path: Path to backtest results CSV
        """
        self.df = pd.read_csv(trades_csv_path)
        self.validate_data()
        
    def validate_data(self):
        """Check required columns exist"""
        required = ['DATE', 'PNL', 'TYPE', 'EXIT_REASON']
        missing = [col for col in required if col not in self.df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
    
    def run_bootstrap(self, num_simulations: int = 10000, seed: int = 42):
        """
        Run day-level bootstrap Monte Carlo simulation
        
        Args:
            num_simulations: Number of bootstrap samples
            seed: Random seed for reproducibility
            
        Returns:
            DataFrame with simulation results
        """
        np.random.seed(seed)
        
        # Group by date to keep CE+PE together
        daily_pnl = self.df.groupby('DATE')['PNL'].sum().values
        unique_dates = self.df['DATE'].unique()
        n_days = len(unique_dates)
        
        print(f"\n🎲 Running Monte Carlo Bootstrap")
        print(f"   Trading days: {n_days}")
        print(f"   Total trades: {len(self.df)}")
        print(f"   Simulations: {num_simulations:,}")
        
        results = []
        
        for sim in range(num_simulations):
            # Randomly sample days WITH replacement
            sampled_indices = np.random.choice(n_days, size=n_days, replace=True)
            sampled_daily_pnl = daily_pnl[sampled_indices]
            
            # Calculate metrics
            total_pnl = sampled_daily_pnl.sum()
            cumulative = np.cumsum(sampled_daily_pnl)
            
            # Drawdown calculation
            running_max = np.maximum.accumulate(cumulative)
            drawdown = running_max - cumulative
            max_dd = drawdown.max()
            
            # Win rate (profitable days)
            win_rate = (sampled_daily_pnl > 0).mean()
            
            # Avg win/loss
            wins = sampled_daily_pnl[sampled_daily_pnl > 0]
            losses = sampled_daily_pnl[sampled_daily_pnl < 0]
            
            avg_win = wins.mean() if len(wins) > 0 else 0
            avg_loss = losses.mean() if len(losses) > 0 else 0
            
            # Profit factor
            total_wins = wins.sum() if len(wins) > 0 else 0
            total_losses = abs(losses.sum()) if len(losses) > 0 else 0
            profit_factor = total_wins / total_losses if total_losses > 0 else np.inf
            
            results.append({
                'simulation': sim,
                'total_pnl': total_pnl,
                'max_drawdown': max_dd,
                'win_rate': win_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': profit_factor,
                'num_winning_days': len(wins),
                'num_losing_days': len(losses)
            })
            
            # Progress
            if (sim + 1) % 1000 == 0:
                print(f"   Progress: {sim + 1:,}/{num_simulations:,}")
        
        return pd.DataFrame(results)
    
    def print_summary(self, mc_results: pd.DataFrame):
        """Print summary statistics"""
        
        print("\n" + "="*70)
        print("MONTE CARLO BOOTSTRAP RESULTS")
        print("="*70)

        
        
        # Total PnL
        print(f"\n📊 TOTAL PnL")
        print(f"   Mean:   {mc_results['total_pnl'].mean():>12,.2f}")
        print(f"   Median: {mc_results['total_pnl'].median():>12,.2f}")
        print(f"   Std:    {mc_results['total_pnl'].std():>12,.2f}")
        print(f"   95% CI: [{mc_results['total_pnl'].quantile(0.025):>10,.2f}, {mc_results['total_pnl'].quantile(0.975):>10,.2f}]")
        
        # Risk metrics
        prob_loss = (mc_results['total_pnl'] < 0).mean() * 100
        prob_profit = (mc_results['total_pnl'] > 0).mean() * 100
        
        print(f"\n⚠️  RISK")
        print(f"   P(Loss):   {prob_loss:>6.1f}%")
        print(f"   P(Profit): {prob_profit:>6.1f}%")
        
        # Max Drawdown
        print(f"\n📉 MAX DRAWDOWN")
        print(f"   Mean:   {mc_results['max_drawdown'].mean():>12,.2f}")
        print(f"   Median: {mc_results['max_drawdown'].median():>12,.2f}")
        print(f"   95% CI: [{mc_results['max_drawdown'].quantile(0.025):>10,.2f}, {mc_results['max_drawdown'].quantile(0.975):>10,.2f}]")
        
        # Win Rate
        print(f"\n🎯 WIN RATE (Daily)")
        print(f"   Mean:   {mc_results['win_rate'].mean()*100:>6.1f}%")
        print(f"   Median: {mc_results['win_rate'].median()*100:>6.1f}%")
        print(f"   95% CI: [{mc_results['win_rate'].quantile(0.025)*100:>5.1f}%, {mc_results['win_rate'].quantile(0.975)*100:>5.1f}%]")
        
        # Avg Win/Loss
        print(f"\n💰 AVG WIN/LOSS (Daily)")
        print(f"   Avg Win:  {mc_results['avg_win'].mean():>12,.2f}")
        print(f"   Avg Loss: {mc_results['avg_loss'].mean():>12,.2f}")
        
        # Profit Factor
        pf_clean = mc_results[mc_results['profit_factor'] != np.inf]['profit_factor']
        if len(pf_clean) > 0:
            print(f"\n📈 PROFIT FACTOR")
            print(f"   Mean:   {pf_clean.mean():>6.2f}")
            print(f"   Median: {pf_clean.median():>6.2f}")
        
        print("\n" + "="*70)
    
    def plot_distributions(self, mc_results: pd.DataFrame, output_dir: str = "output"):
        """Create visualization plots"""
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Monte Carlo Bootstrap Analysis', fontsize=16, fontweight='bold')
        
        # 1. Total PnL Distribution
        ax1 = axes[0, 0]
        ax1.hist(mc_results['total_pnl'], bins=50, alpha=0.7, color='steelblue', edgecolor='black')
        ax1.axvline(mc_results['total_pnl'].mean(), color='red', linestyle='--', linewidth=2, label='Mean')
        ax1.axvline(mc_results['total_pnl'].quantile(0.025), color='orange', linestyle='--', label='95% CI')
        ax1.axvline(mc_results['total_pnl'].quantile(0.975), color='orange', linestyle='--')
        ax1.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.3)
        ax1.set_xlabel('Total PnL', fontsize=11)
        ax1.set_ylabel('Frequency', fontsize=11)
        ax1.set_title('Total PnL Distribution', fontweight='bold')
        ax1.legend()
        ax1.grid(alpha=0.3)
        
        # 2. Max Drawdown Distribution
        ax2 = axes[0, 1]
        ax2.hist(mc_results['max_drawdown'], bins=50, alpha=0.7, color='coral', edgecolor='black')
        ax2.axvline(mc_results['max_drawdown'].mean(), color='red', linestyle='--', linewidth=2, label='Mean')
        ax2.set_xlabel('Max Drawdown', fontsize=11)
        ax2.set_ylabel('Frequency', fontsize=11)
        ax2.set_title('Max Drawdown Distribution', fontweight='bold')
        ax2.legend()
        ax2.grid(alpha=0.3)
        
        # 3. Win Rate Distribution
        ax3 = axes[1, 0]
        ax3.hist(mc_results['win_rate']*100, bins=50, alpha=0.7, color='lightgreen', edgecolor='black')
        ax3.axvline(mc_results['win_rate'].mean()*100, color='red', linestyle='--', linewidth=2, label='Mean')
        ax3.set_xlabel('Win Rate (%)', fontsize=11)
        ax3.set_ylabel('Frequency', fontsize=11)
        ax3.set_title('Win Rate Distribution', fontweight='bold')
        ax3.legend()
        ax3.grid(alpha=0.3)
        
        # 4. Profit Factor Distribution
        ax4 = axes[1, 1]
        pf_clean = mc_results[mc_results['profit_factor'] != np.inf]['profit_factor']
        ax4.hist(pf_clean, bins=50, alpha=0.7, color='plum', edgecolor='black')
        ax4.axvline(pf_clean.mean(), color='red', linestyle='--', linewidth=2, label='Mean')
        ax4.axvline(1.0, color='black', linestyle='-', linewidth=1, alpha=0.3)
        ax4.set_xlabel('Profit Factor', fontsize=11)
        ax4.set_ylabel('Frequency', fontsize=11)
        ax4.set_title('Profit Factor Distribution', fontweight='bold')
        ax4.legend()
        ax4.grid(alpha=0.3)
        
        plt.tight_layout()
        
        plot_path = output_path / "monte_carlo_analysis.png"
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        print(f"\n📊 Plot saved: {plot_path}")
        
        plt.close()
    
    def save_results(self, mc_results: pd.DataFrame, output_path: str):
        """Save simulation results to CSV"""
        mc_results.to_csv(output_path, index=False)
        print(f"💾 Results saved: {output_path}")


def main():
    """Example usage"""
    
    # Path to your backtest results
    TRADES_CSV = "output/nifty_dynamicatminventory_20210601_20251231.csv"
    
    # Initialize analysis
    mc = MonteCarloAnalysis(TRADES_CSV)
        
    # Run bootstrap (10,000 simulations)
    results = mc.run_bootstrap(num_simulations=10000)
    
    # Print summary
    mc.print_summary(results)
    
    # Create plots
    mc.plot_distributions(results)
    
    # Save detailed results
    mc.save_results(results, "output/monte_carlo_results.csv")


if __name__ == "__main__":
    main()