"""
Visualizer Module
Responsible for creating data visualizations and analysis
"""

import os
import yaml
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame


class Visualizer:
    """Class for data visualization"""

    def __init__(self, config_path: str = "config/analysis_config.yaml"):
        """
        Initializes the Visualizer

        Args:
            config_path: Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.viz_config = self.config['visualization']
        self.output_path = self.config['output']['path']

        # Creates output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(os.path.join(self.output_path, 'visualizations'), exist_ok=True)

        # Configures style
        sns.set_style('darkgrid')
        plt.rcParams['figure.figsize'] = self.viz_config['figure_size']
        plt.rcParams['figure.dpi'] = self.viz_config['dpi']

    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Loads configurations from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _save_figure(self, fig, filename: str):
        """Saves figure in the output directory"""
        filepath = os.path.join(self.output_path, 'visualizations', filename)
        fig.savefig(filepath, bbox_inches='tight', dpi=self.viz_config['dpi'])
        print(f"    Saved: {filepath}")
        plt.close(fig)

    def plot_price_history(self, df: DataFrame):
        """
        Plots price history

        Args:
            df: DataFrame with data
        """
        print("\n Creating Price History Plot...")

        # Converts to Pandas for plotting
        pdf = df.select('Date', 'Close', 'Volume').toPandas()
        pdf['Date'] = pd.to_datetime(pdf['Date'])
        pdf = pdf.sort_values('Date')

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

        # Price plot
        ax1.plot(pdf['Date'], pdf['Close'], linewidth=1.5, color='#00b894')
        ax1.set_ylabel('Close Price ($)', fontsize=12, fontweight='bold')
        ax1.set_title('NVIDIA Stock Price History', fontsize=16, fontweight='bold', pad=20)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        # Volume plot
        ax2.bar(pdf['Date'], pdf['Volume'], width=1, color='#0984e3', alpha=0.6)
        ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Volume', fontsize=12, fontweight='bold')
        ax2.set_title('Trading Volume', fontsize=14, fontweight='bold', pad=15)
        ax2.grid(True, alpha=0.3)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.0f}M'))

        plt.tight_layout()
        self._save_figure(fig, 'price_history.png')

    def plot_moving_averages(self, df: DataFrame):
        """
        Plots moving averages

        Args:
            df: DataFrame with moving averages
        """
        print("\n Creating Moving Averages Plot...")

        ma_columns = ['Date', 'Close'] + [col for col in df.columns if col.startswith('SMA_')]
        pdf = df.select(ma_columns).toPandas()
        pdf['Date'] = pd.to_datetime(pdf['Date'])
        pdf = pdf.sort_values('Date')

        fig, ax = plt.subplots(figsize=(14, 8))

        # Plot closing price
        ax.plot(pdf['Date'], pdf['Close'], label='Close Price', linewidth=2, color='black', alpha=0.7)

        # Plot moving averages
        colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6']
        for i, col in enumerate([c for c in ma_columns if c.startswith('SMA_')]):
            ax.plot(pdf['Date'], pdf[col], label=col, linewidth=1.5,
                   color=colors[i % len(colors)], alpha=0.8)

        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Price ($)', fontsize=12, fontweight='bold')
        ax.set_title('NVIDIA Stock Price with Moving Averages', fontsize=16, fontweight='bold', pad=20)
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        plt.tight_layout()
        self._save_figure(fig, 'moving_averages.png')

    def plot_bollinger_bands(self, df: DataFrame):
        """
        Plots Bollinger Bands

        Args:
            df: DataFrame with Bollinger Bands
        """
        print("\n Creating Bollinger Bands Plot...")

        pdf = df.select('Date', 'Close', 'BB_Upper', 'BB_Middle', 'BB_Lower').toPandas()
        pdf['Date'] = pd.to_datetime(pdf['Date'])
        pdf = pdf.sort_values('Date')

        fig, ax = plt.subplots(figsize=(14, 8))

        # Plot bands
        ax.plot(pdf['Date'], pdf['Close'], label='Close Price', linewidth=2, color='black')
        ax.plot(pdf['Date'], pdf['BB_Middle'], label='Middle Band (SMA)',
               linewidth=1.5, color='#3498db', linestyle='--')
        ax.plot(pdf['Date'], pdf['BB_Upper'], label='Upper Band',
               linewidth=1, color='#e74c3c', alpha=0.7)
        ax.plot(pdf['Date'], pdf['BB_Lower'], label='Lower Band',
               linewidth=1, color='#2ecc71', alpha=0.7)

        # Fills area between bands
        ax.fill_between(pdf['Date'], pdf['BB_Upper'], pdf['BB_Lower'],
                       alpha=0.1, color='gray')

        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Price ($)', fontsize=12, fontweight='bold')
        ax.set_title('NVIDIA Stock Price with Bollinger Bands', fontsize=16, fontweight='bold', pad=20)
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        plt.tight_layout()
        self._save_figure(fig, 'bollinger_bands.png')

    def plot_rsi(self, df: DataFrame):
        """
        Plots RSI

        Args:
            df: DataFrame with RSI
        """
        print("\n Creating RSI Plot...")

        pdf = df.select('Date', 'Close', 'RSI').toPandas()
        pdf['Date'] = pd.to_datetime(pdf['Date'])
        pdf = pdf.sort_values('Date')

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

        # Plot price
        ax1.plot(pdf['Date'], pdf['Close'], linewidth=1.5, color='#00b894')
        ax1.set_ylabel('Close Price ($)', fontsize=12, fontweight='bold')
        ax1.set_title('NVIDIA Stock Price', fontsize=14, fontweight='bold', pad=15)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        # Plot RSI
        ax2.plot(pdf['Date'], pdf['RSI'], linewidth=1.5, color='#6c5ce7')
        ax2.axhline(y=70, color='r', linestyle='--', linewidth=1, alpha=0.7, label='Overbought (70)')
        ax2.axhline(y=30, color='g', linestyle='--', linewidth=1, alpha=0.7, label='Oversold (30)')
        ax2.fill_between(pdf['Date'], 30, 70, alpha=0.1, color='gray')

        ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax2.set_ylabel('RSI', fontsize=12, fontweight='bold')
        ax2.set_title('Relative Strength Index (RSI)', fontsize=14, fontweight='bold', pad=15)
        ax2.legend(loc='best', fontsize=10)
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim(0, 100)

        plt.tight_layout()
        self._save_figure(fig, 'rsi_indicator.png')

    def plot_macd(self, df: DataFrame):
        """
        Plots MACD

        Args:
            df: DataFrame with MACD
        """
        print("\n Creating MACD Plot...")

        pdf = df.select('Date', 'Close', 'MACD', 'MACD_Signal', 'MACD_Histogram').toPandas()
        pdf['Date'] = pd.to_datetime(pdf['Date'])
        pdf = pdf.sort_values('Date')

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

        # Plot price
        ax1.plot(pdf['Date'], pdf['Close'], linewidth=1.5, color='#00b894')
        ax1.set_ylabel('Close Price ($)', fontsize=12, fontweight='bold')
        ax1.set_title('NVIDIA Stock Price', fontsize=14, fontweight='bold', pad=15)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        # Plot MACD
        ax2.plot(pdf['Date'], pdf['MACD'], linewidth=1.5, color='#0984e3', label='MACD')
        ax2.plot(pdf['Date'], pdf['MACD_Signal'], linewidth=1.5, color='#e74c3c', label='Signal')

        # Plot histogram
        colors = ['green' if x > 0 else 'red' for x in pdf['MACD_Histogram']]
        ax2.bar(pdf['Date'], pdf['MACD_Histogram'], width=1, color=colors, alpha=0.3, label='Histogram')

        ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5, alpha=0.5)
        ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax2.set_ylabel('MACD Value', fontsize=12, fontweight='bold')
        ax2.set_title('MACD Indicator', fontsize=14, fontweight='bold', pad=15)
        ax2.legend(loc='best', fontsize=10)
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        self._save_figure(fig, 'macd_indicator.png')

    def plot_yearly_performance(self, yearly_df: DataFrame):
        """
        Plots yearly performance

        Args:
            yearly_df: DataFrame with yearly analysis
        """
        print("\n Creating Yearly Performance Plot...")

        pdf = yearly_df.toPandas()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Plot average price by year
        ax1.bar(pdf['Year'], pdf['Avg_Close'], color='#00b894', alpha=0.8)
        ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Close Price ($)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Annual Stock Price', fontsize=14, fontweight='bold', pad=15)
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        # Plot yearly return
        returns_data = pdf[pdf['Yearly_Return'].notna()]
        colors = ['green' if x > 0 else 'red' for x in returns_data['Yearly_Return']]
        ax2.bar(returns_data['Year'], returns_data['Yearly_Return'], color=colors, alpha=0.8)
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax2.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Yearly Return (%)', fontsize=12, fontweight='bold')
        ax2.set_title('Annual Returns', fontsize=14, fontweight='bold', pad=15)
        ax2.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        self._save_figure(fig, 'yearly_performance.png')

    def plot_volatility_analysis(self, df: DataFrame):
        """
        Plots volatility analysis

        Args:
            df: DataFrame with volatility metrics
        """
        print("\n Creating Volatility Analysis Plot...")

        vol_columns = ['Date', 'Close'] + [col for col in df.columns if col.startswith('Volatility_')]
        pdf = df.select(vol_columns).toPandas()
        pdf['Date'] = pd.to_datetime(pdf['Date'])
        pdf = pdf.sort_values('Date')

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

        # Plot price
        ax1.plot(pdf['Date'], pdf['Close'], linewidth=1.5, color='#00b894')
        ax1.set_ylabel('Close Price ($)', fontsize=12, fontweight='bold')
        ax1.set_title('NVIDIA Stock Price', fontsize=14, fontweight='bold', pad=15)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

        # Plot volatility
        colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12']
        for i, col in enumerate([c for c in vol_columns if c.startswith('Volatility_')]):
            ax2.plot(pdf['Date'], pdf[col], label=col.replace('Volatility_', ''),
                    linewidth=1.5, color=colors[i % len(colors)], alpha=0.8)

        ax2.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Volatility (%)', fontsize=12, fontweight='bold')
        ax2.set_title('Historical Volatility', fontsize=14, fontweight='bold', pad=15)
        ax2.legend(loc='best', fontsize=10)
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        self._save_figure(fig, 'volatility_analysis.png')

    def plot_weekday_patterns(self, weekday_df: DataFrame):
        """
        Plots weekday patterns

        Args:
            weekday_df: DataFrame with weekday analysis
        """
        print("\n Creating Weekday Patterns Plot...")

        pdf = weekday_df.toPandas()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Plot average return by weekday
        colors = ['green' if x > 0 else 'red' for x in pdf['Avg_Return']]
        ax1.bar(pdf['WeekDay'], pdf['Avg_Return'], color=colors, alpha=0.8)
        ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax1.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Return (%)', fontsize=12, fontweight='bold')
        ax1.set_title('Average Returns by Weekday', fontsize=14, fontweight='bold', pad=15)
        ax1.grid(True, alpha=0.3, axis='y')
        ax1.tick_params(axis='x', rotation=45)

        # Plot average volume by weekday
        ax2.bar(pdf['WeekDay'], pdf['Avg_Volume'], color='#0984e3', alpha=0.8)
        ax2.set_xlabel('Day of Week', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Average Volume', fontsize=12, fontweight='bold')
        ax2.set_title('Average Trading Volume by Weekday', fontsize=14, fontweight='bold', pad=15)
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.tick_params(axis='x', rotation=45)
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.0f}M'))

        plt.tight_layout()
        self._save_figure(fig, 'weekday_patterns.png')

    def create_all_visualizations(self, df: DataFrame, analysis_results: dict):
        """
        Creates all visualizations

        Args:
            df: Main DataFrame
            analysis_results: Analysis results
        """
        print("\n" + "="*70)
        print(" Creating All Visualizations")
        print("="*70)

        self.plot_price_history(df)

        if 'SMA_50' in df.columns:
            self.plot_moving_averages(df)

        if 'BB_Upper' in df.columns:
            self.plot_bollinger_bands(df)

        if 'RSI' in df.columns:
            self.plot_rsi(df)

        if 'MACD' in df.columns:
            self.plot_macd(df)

        if 'Volatility_30d' in df.columns:
            self.plot_volatility_analysis(df)

        if 'yearly_analysis' in analysis_results:
            self.plot_yearly_performance(analysis_results['yearly_analysis'])

        if 'weekday_analysis' in analysis_results:
            self.plot_weekday_patterns(analysis_results['weekday_analysis'])

        print("\n" + "="*70)
        print(" All Visualizations Created!")
        print(f"   Saved to: {os.path.join(self.output_path, 'visualizations')}")
        print("="*70)