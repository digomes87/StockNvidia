"""
Analyzer Module
Responsible for statistical analysis and business insights.
"""

import yaml
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, max as spark_max, min as spark_min,
    stddev, percentile_approx, countDistinct, year, month,
    when, datediff, current_date, lag
)
from pyspark.sql import Window


def _load_config(config_path: str) -> dict:
    """Load settings from file. YAML"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_overall_statistics(df: DataFrame) -> dict:
    """
    Calculates general statistics for the dataset.

    Args:
        df: DataFrame with data

    Returns:
        Dic with statistics
    """
    print("\n Calculating Overall Statistics...")

    stats = df.select(
        count('*').alias('total_records'),
        spark_min('Close').alias('min_price'),
        spark_max('Close').alias('max_price'),
        avg('Close').alias('avg_price'),
        stddev('Close').alias('std_price'),
        spark_sum('Volume').alias('total_volume'),
        avg('Volume').alias('avg_volume'),
        spark_min('Date').alias('start_date'),
        spark_max('Date').alias('end_date')
    ).collect()[0]

    stats_dict = stats.asDict()

    print("\n Overall Statistics:")
    print(f"   Period: {stats_dict['start_date']} to {stats_dict['end_date']}")
    print(f"   Total Records: {stats_dict['total_records']:,}")

    if stats_dict['total_records'] > 0:
        print(f"   Price Range: ${stats_dict['min_price']:.2f} - ${stats_dict['max_price']:.2f}")
        print(f"   Average Price: ${stats_dict['avg_price']:.2f}")
        print(f"   Average Volume: {stats_dict['avg_volume']:,.0f}")
    else:
        print("   No records found.")

    return stats_dict


def analyze_by_year(df: DataFrame) -> DataFrame:
    """
    Aggregate analysis by year

    Args:
        df: DataFrame with data

    Returns:
        DataFrame with annual analysis
    """
    print("\n Analyzing by Year...")

    yearly_analysis = df.groupBy('Symbol', 'Year').agg(
        count('*').alias('Trading_Days'),
        avg('Close').alias('Avg_Close'),
        spark_min('Close').alias('Min_Close'),
        spark_max('Close').alias('Max_Close'),
        stddev('Close').alias('Volatility'),
        spark_sum('Volume').alias('Total_Volume'),
        avg('Daily_Return_Close').alias('Avg_Daily_Return'),
        spark_min('Daily_Return_Close').alias('Min_Daily_Return'),
        spark_max('Daily_Return_Close').alias('Max_Daily_Return')
    ).orderBy('Year')

    # Calculates annual return.
    window_spec = Window.partitionBy('Symbol').orderBy('Year')

    yearly_analysis = yearly_analysis.withColumn(
        'Prev_Avg_Close', lag('Avg_Close').over(window_spec)
    ).withColumn(
        'Yearly_Return',
        when(col('Prev_Avg_Close').isNotNull(),
             ((col('Avg_Close') - col('Prev_Avg_Close')) / col('Prev_Avg_Close')) * 100
        ).otherwise(None)
    ).drop('Prev_Avg_Close')

    print("    Yearly analysis complete")

    return yearly_analysis


def analyze_by_month(df: DataFrame) -> DataFrame:
    """
    Aggregate analysis by month

    Args:
        df: DataFrame with data

    Returns:
        DataFrame with monthly analysis
    """
    print("\n Analyzing by Month...")

    monthly_analysis = df.groupBy('Symbol', 'Year', 'Month', 'MonthName').agg(
        count('*').alias('Trading_Days'),
        avg('Close').alias('Avg_Close'),
        spark_min('Close').alias('Min_Close'),
        spark_max('Close').alias('Max_Close'),
        stddev('Close').alias('Volatility'),
        spark_sum('Volume').alias('Total_Volume'),
        avg('Daily_Return_Close').alias('Avg_Daily_Return')
    )

    # Calculates monthly return
    window_spec = Window.partitionBy('Symbol').orderBy('Year', 'Month')

    monthly_analysis = monthly_analysis.withColumn(
        'Prev_Avg_Close', lag(col('Avg_Close'), 1).over(window_spec)
    ).withColumn(
        'Monthly_Return',
        when(col('Prev_Avg_Close').isNotNull(),
             ((col('Avg_Close') - col('Prev_Avg_Close')) /
              col('Prev_Avg_Close')) * 100
        ).otherwise(None)
    ).drop('Prev_Avg_Close').orderBy('Year', 'Month')

    print("    Monthly analysis complete")

    return monthly_analysis


def analyze_by_quarter(df: DataFrame) -> DataFrame:
    """
    Aggregate analysis by quarter

    Args:
        df: DataFrame with data

    Returns:
        DataFrame with quarterly analysis
    """
    print("\n Analyzing by Quarter...")

    quarterly_analysis = df.groupBy('Year', 'Quarter').agg(
        count('*').alias('Trading_Days'),
        avg('Close').alias('Avg_Close'),
        spark_min('Close').alias('Min_Close'),
        spark_max('Close').alias('Max_Close'),
        stddev('Close').alias('Volatility'),
        spark_sum('Volume').alias('Total_Volume'),
        avg('Daily_Return_Close').alias('Avg_Daily_Return')
    ).orderBy('Year', 'Quarter')

    print("    Quarterly analysis complete")

    return quarterly_analysis


def analyze_weekday_patterns(df: DataFrame) -> DataFrame:
    """
    Analysis of patterns by day of the week

    Args:
        df: DataFrame with data

    Returns:
        DataFrame with analysis by day of the week
    """
    print("\n Analyzing Weekday Patterns...")

    weekday_analysis = df.groupBy('DayOfWeek', 'WeekDay').agg(
        count('*').alias('Total_Days'),
        avg('Daily_Return_Close').alias('Avg_Return'),
        stddev('Daily_Return_Close').alias('Return_Volatility'),
        avg('Volume').alias('Avg_Volume'),
        avg('Daily_Range').alias('Avg_Range')
    ).orderBy('DayOfWeek')

    print("    Weekday pattern analysis complete")

    return weekday_analysis


def identify_best_worst_days(df: DataFrame, top_n: int = 10) -> dict:
    """
    Identifies best and worst trading days

    Args:
        df: DataFrame with data
        top_n: Number of days to return

    Returns:
        Dictionary with best and worst days
    """
    print(f"\n Identifying Top {top_n} Best/Worst Trading Days...")

    best_days = df.select(
        'Date', 'Close', 'Daily_Return_Close', 'Volume'
    ).orderBy(col('Daily_Return_Close').desc()).limit(top_n)

    worst_days = df.select(
        'Date', 'Close', 'Daily_Return_Close', 'Volume'
    ).orderBy(col('Daily_Return_Close').asc()).limit(top_n)

    print(f"    Best/Worst {top_n} days identified")

    return {
        'best_days': best_days,
        'worst_days': worst_days
    }


def calculate_risk_metrics(df: DataFrame) -> dict:
    """
    Calculates risk metrics

    Args:
        df: DataFrame with data

    Returns:
        Dictionary with risk metrics
    """
    print("\n  Calculating Risk Metrics...")

    # Filters non-null return data
    df_returns = df.filter(col('Daily_Return_Close').isNotNull())

    risk_metrics = df_returns.agg(
        avg('Daily_Return_Close').alias('mean_return'),
        stddev('Daily_Return_Close').alias('volatility'),
        spark_min('Daily_Return_Close').alias('max_loss'),
        spark_max('Daily_Return_Close').alias('max_gain'),
        percentile_approx('Daily_Return_Close', 0.05).alias('var_5pct'),
        percentile_approx('Daily_Return_Close', 0.01).alias('var_1pct')
    ).collect()[0]

    metrics = risk_metrics.asDict()

    # Calculates Sharpe Ratio (assuming risk-free rate of 2%)
    risk_free_rate = 2.0 / 252  # Daily risk-free rate
    if metrics['volatility'] and metrics['volatility'] > 0:
        metrics['sharpe_ratio'] = (metrics['mean_return'] - risk_free_rate) / metrics['volatility']
    else:
        metrics['sharpe_ratio'] = None

    print("\n Risk Metrics:")
    print(f"   Mean Daily Return: {metrics['mean_return']:.4f}%")
    print(f"   Daily Volatility: {metrics['volatility']:.4f}%")
    print(f"   Value at Risk (5%): {metrics['var_5pct']:.4f}%")
    print(f"   Value at Risk (1%): {metrics['var_1pct']:.4f}%")
    print(f"   Sharpe Ratio: {metrics['sharpe_ratio']:.4f}" if metrics['sharpe_ratio'] else "   Sharpe Ratio: N/A")

    return metrics


def analyze_volume_trends(df: DataFrame) -> DataFrame:
    """
    Analyzes volume trends

    Args:
        df: DataFrame with data

    Returns:
        DataFrame with volume analysis
    """
    print("\n Analyzing Volume Trends...")

    volume_analysis = df.groupBy('Year').agg(
        avg('Volume').alias('Avg_Volume'),
        spark_min('Volume').alias('Min_Volume'),
        spark_max('Volume').alias('Max_Volume'),
        stddev('Volume').alias('Volume_Volatility')
    ).orderBy('Year')

    print("    Volume trend analysis complete")

    return volume_analysis


def analyze_technical_signals(df: DataFrame) -> dict:
    """
    Analyzes current technical signals

    Args:
        df: DataFrame with data and technical indicators

    Returns:
        Dictionary with technical signals
    """
    print("\n Analyzing Technical Signals...")

    # Gets most recent data
    latest = df.orderBy(col('Date').desc()).limit(1).collect()[0]

    signals = {
        'date': latest['Date'],
        'close_price': latest['Close'],
        'rsi': latest['RSI'] if 'RSI' in df.columns else None,
        'macd': latest['MACD'] if 'MACD' in df.columns else None,
        'macd_signal': latest['MACD_Signal'] if 'MACD_Signal' in df.columns else None,
        'bb_position': latest['BB_Position'] if 'BB_Position' in df.columns else None,
    }

    # Determine signals
    if signals['rsi']:
        if signals['rsi'] < 30:
            signals['rsi_signal'] = 'OVERSOLD - Buy Signal'
        elif signals['rsi'] > 70:
            signals['rsi_signal'] = 'OVERBOUGHT - Sell Signal'
        else:
            signals['rsi_signal'] = 'NEUTRAL'

    if signals['macd'] and signals['macd_signal']:
        if signals['macd'] > signals['macd_signal']:
            signals['macd_signal_interpretation'] = 'BULLISH'
        else:
            signals['macd_signal_interpretation'] = 'BEARISH'

    print("\n Technical Signals:")
    print(f"Date: {signals['date']}")
    print(f"Close Price: ${signals['close_price']:.2f}")
    if signals['rsi']:
        print(f"RSI: {signals['rsi']:.2f} - {signals.get('rsi_signal', 'N/A')}")
    if signals['macd']:
        print(f"MACD: {signals.get('macd_signal_interpretation', 'N/A')}")

    return signals


def run_comprehensive_analysis(df: DataFrame) -> dict:
    """
    Executes comprehensive analysis

    Args:
        df: DataFrame with complete data

    Returns:
        Dictionary with all results
    """
    print("\n" + "="*70)
    print(" Starting Comprehensive Analysis")
    print("="*70)

    results = {
        'overall_stats': get_overall_statistics(df),
        'yearly_analysis': analyze_by_year(df),
        'monthly_analysis': analyze_by_month(df),
        'quarterly_analysis': analyze_by_quarter(df),
        'weekday_analysis': analyze_weekday_patterns(df),
        'best_worst_days': identify_best_worst_days(df),
        'risk_metrics': calculate_risk_metrics(df),
        'volume_analysis': analyze_volume_trends(df),
        'technical_signals': analyze_technical_signals(df)
    }

    print("\n" + "="*70)
    print("Comprehensive Analysis Complete!")
    print("="*70)

    return results


class StockAnalyzer:
    """Class for advanced stock analysis"""

    def __init__(self, config_path: str = "config/analysis_config.yaml"):
        """
        Initialize StockAnalyzer

        Args:
            config_path: Path to the configuration file
        """
        self.config = _load_config(config_path)
        self.analysis_config = self.config['analysis']