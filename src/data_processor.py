"""
Data Processor Module
Responsible for processing and cleaning dataset data
"""

import yaml
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, dayofweek,
    date_format, datediff, current_date, lag, when, lit
)


class DataProcessor:
    """Class to process and transform data"""

    def __init__(self, config_path: str = "config/analysis_config.yaml"):
        """
        Initializes DataProcessor

        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.data_config = self.config['data']

    def _load_config(self, config_path: str) -> dict:
        """Loads configurations from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans data removing nulls and duplicates

        Args:
            df: Original DataFrame

        Returns:
            Cleaned DataFrame
        """
        print("\n Cleaning data...")

        initial_count = df.count()

        # Removes rows with null values in critical columns
        df_clean = df.dropna(subset=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

        # Removes duplicates based on date
        df_clean = df_clean.dropDuplicates(['Date'])

        # Adds Symbol column for partitioning
        df_clean = df_clean.withColumn('Symbol', lit('NVDA'))

        final_count = df_clean.count()
        removed = initial_count - final_count

        print(f"   Removed {removed} rows ({removed/initial_count*100:.2f}%)")
        print(f"   Final dataset: {final_count:,} rows")

        return df_clean

    def convert_date_column(self, df: DataFrame) -> DataFrame:
        """
        Converts date column to Date type

        Args:
            df: DataFrame with date column as string

        Returns:
            DataFrame with converted date column
        """
        date_col = self.data_config['date_column']
        date_format = self.data_config['date_format']

        print(f"\n Converting date column '{date_col}'...")

        df_converted = df.withColumn(
            date_col,
            to_date(col(date_col), date_format)
        )

        print("    Date conversion complete")

        return df_converted

    def add_temporal_features(self, df: DataFrame) -> DataFrame:
        """
        Adds temporal features (year, month, day, day of week)

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with added temporal features
        """
        print("\n Adding temporal features...")

        date_col = self.data_config['date_column']

        df_temporal = df.withColumn('Year', year(col(date_col))) \
                       .withColumn('Month', month(col(date_col))) \
                       .withColumn('Day', dayofmonth(col(date_col))) \
                       .withColumn('DayOfWeek', dayofweek(col(date_col))) \
                       .withColumn('WeekDay', date_format(col(date_col), 'EEEE')) \
                       .withColumn('MonthName', date_format(col(date_col), 'MMMM')) \
                       .withColumn('Quarter',
                                  when(col('Month').isin([1,2,3]), 1)
                                  .when(col('Month').isin([4,5,6]), 2)
                                  .when(col('Month').isin([7,8,9]), 3)
                                  .otherwise(4))

        print("    Temporal features added:")
        print("      - Year, Month, Day")
        print("      - DayOfWeek, WeekDay")
        print("      - MonthName, Quarter")

        return df_temporal

    def add_price_features(self, df: DataFrame) -> DataFrame:
        """
        Adds price-related features

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with added price features
        """
        print("\n Adding price features...")

        df_price = df.withColumn(
            'Daily_Range', col('High') - col('Low')
        ).withColumn(
            'Daily_Return', (col('Close') - col('Open')) / col('Open') * 100
        ).withColumn(
            'Price_Change', col('Close') - col('Open')
        ).withColumn(
            'High_Low_Ratio', col('High') / col('Low')
        ).withColumn(
            'Close_Open_Ratio', col('Close') / col('Open')
        )

        # Adds previous price
        window_spec = Window.partitionBy('Symbol').orderBy('Date')

        df_price = df_price.withColumn(
            'Previous_Close', lag('Close', 1).over(window_spec)
        ).withColumn(
            'Daily_Return_Close',
            when(col('Previous_Close').isNotNull(),
                 (col('Close') - col('Previous_Close')) / col('Previous_Close') * 100)
            .otherwise(None)
        )

        print("    Price features added:")
        print("      - Daily_Range, Daily_Return, Price_Change")
        print("      - High_Low_Ratio, Close_Open_Ratio")
        print("      - Previous_Close, Daily_Return_Close")

        return df_price

    def add_volume_features(self, df: DataFrame) -> DataFrame:
        """
        Adds volume-related features

        Args:
            df: Original DataFrame

        Returns:
            DataFrame with added volume features
        """
        print("\n Adding volume features...")

        window_spec = Window.partitionBy('Symbol').orderBy('Date')

        df_volume = df.withColumn(
            'Previous_Volume', lag('Volume', 1).over(window_spec)
        ).withColumn(
            'Volume_Change',
            when(col('Previous_Volume').isNotNull(),
                 (col('Volume') - col('Previous_Volume')) / col('Previous_Volume') * 100)
            .otherwise(None)
        )

        print("    Volume features added:")
        print("      - Previous_Volume, Volume_Change")

        return df_volume

    def sort_by_date(self, df: DataFrame) -> DataFrame:
        """
        Sorts DataFrame by date

        Args:
            df: Unsorted DataFrame

        Returns:
            DataFrame sorted by date
        """
        date_col = self.data_config['date_column']
        return df.orderBy(col(date_col))

    def process_pipeline(self, df: DataFrame) -> DataFrame:
        """
        Executes complete processing pipeline

        Args:
            df: Raw DataFrame

        Returns:
            Processed DataFrame
        """
        print("\n" + "="*70)
        print(" Starting Data Processing Pipeline")
        print("="*70)

        # Processing pipeline
        df_processed = (df
            .transform(self.clean_data)
            .transform(self.convert_date_column)
            .transform(self.add_temporal_features)
            .transform(self.add_price_features)
            .transform(self.add_volume_features)
            .transform(self.sort_by_date)
        )

        print("\n" + "="*70)
        print(" Data Processing Pipeline Complete!")
        print("="*70)

        return df_processed

    def get_date_range(self, df: DataFrame) -> dict:
        """
        Gets dataset date range

        Args:
            df: Processed DataFrame

        Returns:
            Dictionary with min and max dates
        """
        from pyspark.sql.functions import min as spark_min, max as spark_max

        date_col = self.data_config['date_column']

        date_range = df.select(
            spark_min(col(date_col)).alias('min_date'),
            spark_max(col(date_col)).alias('max_date')
        ).collect()[0]

        return {
            'min_date': date_range['min_date'],
            'max_date': date_range['max_date']
        }