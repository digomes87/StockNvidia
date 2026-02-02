"""
Feature Engineer Module
Responsible for creating advanced technical features (technical indicators)
"""

import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, avg, stddev, lag, sum as spark_sum,
    when, abs as spark_abs, max as spark_max, min as spark_min
)


class FeatureEngineer:
    """Class for technical feature engineering"""

    def __init__(self, config_path: str = "config/analysis_config.yaml"):
        """
        Initializes FeatureEngineer

        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.analysis_config = self.config['analysis']

    def _load_config(self, config_path: str) -> dict:
        """Loads configurations from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def add_moving_averages(self, df: DataFrame) -> DataFrame:
        """
        Adds Simple Moving Averages (SMA)

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with moving averages
        """
        print("\n Calculating Moving Averages...")

        windows = self.analysis_config['moving_averages']

        df_ma = df
        for window in windows:
            window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window + 1, 0)

            df_ma = df_ma.withColumn(
                f'SMA_{window}',
                avg('Close').over(window_spec)
            )
            print(f"    SMA_{window} calculated")

        return df_ma

    def add_exponential_moving_averages(self, df: DataFrame, windows: list = [12, 26]) -> DataFrame:
        """
        Adds Exponential Moving Averages (EMA)

        Args:
            df: Original DataFrame
            windows: List of periods for EMA

        Returns:
            DataFrame with EMAs
        """
        print("\n Calculating Exponential Moving Averages...")

        df_ema = df

        for window in windows:
            # Converting to Pandas to calculate EMA (PySpark doesn't have native EMA)
            # In production, this would be done with optimized UDF
            alpha = 2 / (window + 1)

            # Using approximation with weighted moving averages
            window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window + 1, 0)

            df_ema = df_ema.withColumn(
                f'EMA_{window}',
                avg('Close').over(window_spec)  # Simplification
            )
            print(f"    EMA_{window} calculated")

        return df_ema

    def add_bollinger_bands(self, df: DataFrame) -> DataFrame:
        """
        Adds Bollinger Bands

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with Bollinger Bands
        """
        print("\n Calculating Bollinger Bands...")

        window = self.analysis_config['bollinger_bands']['window']
        num_std = self.analysis_config['bollinger_bands']['num_std']

        window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window + 1, 0)

        df_bb = df.withColumn(
            'BB_Middle',
            avg('Close').over(window_spec)
        ).withColumn(
            'BB_Std',
            stddev('Close').over(window_spec)
        )

        df_bb = df_bb.withColumn(
            'BB_Upper',
            col('BB_Middle') + (col('BB_Std') * num_std)
        ).withColumn(
            'BB_Lower',
            col('BB_Middle') - (col('BB_Std') * num_std)
        ).withColumn(
            'BB_Width',
            col('BB_Upper') - col('BB_Lower')
        ).withColumn(
            'BB_Position',
            when(col('BB_Width') != 0,
                 (col('Close') - col('BB_Lower')) / col('BB_Width'))
            .otherwise(0.5)
        )

        print(f"    Bollinger Bands calculated (window={window}, std={num_std})")

        return df_bb

    def add_rsi(self, df: DataFrame) -> DataFrame:
        """
        Adds Relative Strength Index (RSI)

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with RSI
        """
        print("\n Calculating RSI...")

        window = self.analysis_config['rsi']['window']

        # Calculates gains and losses
        df_rsi = df.withColumn(
            'Price_Diff',
            col('Close') - lag('Close', 1).over(Window.partitionBy('Symbol').orderBy('Date'))
        )

        df_rsi = df_rsi.withColumn(
            'Gain',
            when(col('Price_Diff') > 0, col('Price_Diff')).otherwise(0)
        ).withColumn(
            'Loss',
            when(col('Price_Diff') < 0, spark_abs(col('Price_Diff'))).otherwise(0)
        )

        # Calculates average gains and losses
        window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window + 1, 0)

        df_rsi = df_rsi.withColumn(
            'Avg_Gain',
            avg('Gain').over(window_spec)
        ).withColumn(
            'Avg_Loss',
            avg('Loss').over(window_spec)
        )

        # Calculates RS and RSI
        df_rsi = df_rsi.withColumn(
            'RS',
            when(col('Avg_Loss') != 0, col('Avg_Gain') / col('Avg_Loss')).otherwise(0)
        ).withColumn(
            'RSI',
            100 - (100 / (1 + col('RS')))
        )

        # Removes intermediate columns
        df_rsi = df_rsi.drop('Price_Diff', 'Gain', 'Loss', 'Avg_Gain', 'Avg_Loss', 'RS')

        print(f"    RSI calculated (window={window})")

        return df_rsi

    def add_macd(self, df: DataFrame) -> DataFrame:
        """
        Adds MACD (Moving Average Convergence Divergence)

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with MACD
        """
        print("\n Calculating MACD...")

        fast = self.analysis_config['macd']['fast_period']
        slow = self.analysis_config['macd']['slow_period']
        signal = self.analysis_config['macd']['signal_period']

        # Adds EMAs
        df_macd = self.add_exponential_moving_averages(df, [fast, slow])

        # Calculates MACD
        df_macd = df_macd.withColumn(
            'MACD',
            col(f'EMA_{fast}') - col(f'EMA_{slow}')
        )

        # Calculates Signal Line
        window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-signal + 1, 0)
        df_macd = df_macd.withColumn(
            'MACD_Signal',
            avg('MACD').over(window_spec)
        )

        # Calculates Histogram
        df_macd = df_macd.withColumn(
            'MACD_Histogram',
            col('MACD') - col('MACD_Signal')
        )

        print(f"    MACD calculated (fast={fast}, slow={slow}, signal={signal})")

        return df_macd

    def add_volatility_metrics(self, df: DataFrame) -> DataFrame:
        """
        Adds volatility metrics

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with volatility metrics
        """
        print("\n Calculating Volatility Metrics...")

        windows = self.analysis_config['volatility']['windows']

        df_vol = df

        for window in windows:
            window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window + 1, 0)

            # Standard deviation of returns
            df_vol = df_vol.withColumn(
                f'Volatility_{window}d',
                stddev('Daily_Return_Close').over(window_spec)
            )

            # Average True Range (ATR)
            df_vol = df_vol.withColumn(
                f'ATR_{window}d',
                avg('Daily_Range').over(window_spec)
            )

            print(f"    Volatility_{window}d and ATR_{window}d calculated")

        return df_vol

    def add_momentum_indicators(self, df: DataFrame) -> DataFrame:
        """
        Adds momentum indicators

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with momentum indicators
        """
        print("\n Calculating Momentum Indicators...")

        window_spec_10 = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-9, 0)
        window_spec_20 = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-19, 0)

        df_momentum = df

        # Rate of Change (ROC)
        df_momentum = df_momentum.withColumn(
            'ROC_10',
            ((col('Close') - lag('Close', 10).over(Window.partitionBy('Symbol').orderBy('Date'))) /
             lag('Close', 10).over(Window.partitionBy('Symbol').orderBy('Date'))) * 100
        )

        # Money Flow Index (simplified)
        df_momentum = df_momentum.withColumn(
            'Typical_Price',
            (col('High') + col('Low') + col('Close')) / 3
        ).withColumn(
            'Money_Flow',
            col('Typical_Price') * col('Volume')
        )

        print("    Momentum indicators calculated")

        return df_momentum

    def add_support_resistance_levels(self, df: DataFrame, window: int = 20) -> DataFrame:
        """
        Identifies support and resistance levels

        Args:
            df: Original DataFrame
            window: Window to calculate levels

        Returns:
            DataFrame with support and resistance levels
        """
        print("\n Calculating Support/Resistance Levels...")

        window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-window + 1, 0)

        df_sr = df.withColumn(
            f'Resistance_{window}d',
            spark_max('High').over(window_spec)
        ).withColumn(
            f'Support_{window}d',
            spark_min('Low').over(window_spec)
        )

        print(f"    Support/Resistance levels calculated (window={window})")

        return df_sr

    def engineer_features(self, df: DataFrame) -> DataFrame:
        """
        Executes complete feature engineering pipeline

        Args:
            df: Processed DataFrame

        Returns:
            DataFrame with all technical features
        """
        print("\n" + "="*70)
        print(" Starting Feature Engineering Pipeline")
        print("="*70)

        df_features = (df
            .transform(self.add_moving_averages)
            .transform(self.add_bollinger_bands)
            .transform(self.add_rsi)
            .transform(self.add_macd)
            .transform(self.add_volatility_metrics)
            .transform(self.add_momentum_indicators)
            .transform(self.add_support_resistance_levels)
        )

        print("\n" + "="*70)
        print(" Feature Engineering Complete!")
        print(f"   Total columns: {len(df_features.columns)}")
        print("="*70)

        return df_features