"""
Data Loader Module
Responsible for loading NVIDIA dataset data using PySpark
"""
import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import kagglehub


class DataLoader:
    """Class to load and validate dataset data"""

    def __init__(self, spark: SparkSession, config_path: str = "config/analysis_config.yaml"):
        """
        Initializes DataLoader

        Args:
            spark: Active SparkSession
            config_path: Path to configuration file
        """
        self.spark = spark
        self.config = self._load_config(config_path)
        self.data_config = self.config['data']

    @staticmethod
    def _load_config(config_path: str) -> dict:
        """Loads configurations from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def download_dataset(self) -> str:
        """
        Downloads dataset from Kaggle

        Returns:
            Path to dataset directory
        """
        print(f"Downloading dataset: {self.data_config['dataset_name']}")
        path = kagglehub.dataset_download(self.data_config['dataset_name'])
        print(f"Dataset downloaded to: {path}")
        return path

    @staticmethod
    def define_schema() -> StructType:
        """
        Defines NVIDIA dataset schema
        
        Returns:
            StructType with defined schema
        """
        return StructType([
            StructField("Date", StringType(), True),
            StructField("Close", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Open", DoubleType(), True),
            StructField("Volume", DoubleType(), True)
        ])

    def load_data(self, dataset_path: str = None) -> 'pyspark.sql.DataFrame':
        """
        Loads CSV data using PySpark

        Args:
            dataset_path: Path to dataset (if None, downloads it)

        Returns:
            PySpark DataFrame with loaded data
        """
        if dataset_path is None:
            dataset_path = self.download_dataset()

        # Searches for CSV file in directory
        csv_files = [f for f in os.listdir(dataset_path) if f.endswith('.csv')]

        if not csv_files:
            raise FileNotFoundError(f"No CSV file found in {dataset_path}")

        csv_path = os.path.join(dataset_path, csv_files[0])
        print(f" Loading data from: {csv_path}")

        # Define schema
        schema = self.define_schema()

        # Loads data with schema
        df = self.spark.read.csv(
            csv_path,
            header=True,
            schema=schema,
            inferSchema=False
        )

        print(f" Data loaded successfully!")
        print(f"   Total records: {df.count():,}")
        print(f"   Columns: {len(df.columns)}")

        return df

    @staticmethod
    def validate_data(df: 'pyspark.sql.DataFrame') -> dict:
        """
        Validates loaded data quality

        Args:
            df: DataFrame to be validated

        Returns:
            Dictionary with validation statistics
        """
        from pyspark.sql.functions import col, count, when, isnan

        print("\n Validating data quality...")

        validation_stats = {
            'total_rows': df.count(),
            'columns': df.columns,
            'null_counts': {},
            'duplicate_dates': 0
        }

        # Counts null values per column
        for column in df.columns:
            condition = col(column).isNull()
            if column != 'Date':
                condition = condition | isnan(col(column))
            
            null_count = df.filter(condition).count()
            validation_stats['null_counts'][column] = null_count

            if null_count > 0:
                print(f"     Column '{column}': {null_count} null values")

        # Checks for date duplicates
        duplicate_count = df.groupBy('Date').count().filter(col('count') > 1).count()
        validation_stats['duplicate_dates'] = duplicate_count

        if duplicate_count > 0:
            print(f"     Found {duplicate_count} duplicate dates")

        print(" Validation complete!")

        return validation_stats

    @staticmethod
    def get_data_summary(df: 'pyspark.sql.DataFrame') -> None:
        """
        Displays a summary of loaded data

        Args:
            df: DataFrame to summarize
        """
        print("\n Data Summary:")
        print("=" * 70)

        # Shows schema
        print("\nSchema:")
        df.printSchema()

        # Shows first rows
        print("\nFirst 5 rows:")
        df.show(5, truncate=False)

        # Descriptive statistics
        print("\nDescriptive Statistics:")
        df.describe().show()

        print("=" * 70)