"""
NVIDIA Stock Analysis Pipeline - Main Script
Complete data analysis pipeline for NVIDIA stock using PySpark
"""

import os
import yaml
from pyspark.sql import SparkSession
from src import DataLoader, DataProcessor, FeatureEngineer, StockAnalyzer, Visualizer, run_comprehensive_analysis


def create_spark_session(config_path: str = "config/spark_config.yaml") -> SparkSession:
    """
    Creates and configures SparkSession

    Args:
        config_path: Path to Spark configuration

    Returns:
        Configured SparkSession
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    spark_config = config['spark']

    print("\n" + "="*70)
    print(" Initializing Spark Session")
    print("="*70)

    builder = SparkSession.builder \
        .appName(spark_config['app_name']) \
        .master(spark_config['master'])

    # Adds memory configurations
    builder = builder.config("spark.driver.memory", spark_config['driver_memory']) \
                    .config("spark.executor.memory", spark_config['executor_memory'])

    # Adds SQL configurations
    for key, value in spark_config['sql'].items():
        builder = builder.config(f"spark.sql.{key}", value)

    # Adds additional configurations
    for key, value in spark_config['config'].items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_config['log_level'])

    print(f"Spark Session created: {spark_config['app_name']}")
    print(f"Version: {spark.version}")
    print(f"Master: {spark_config['master']}")
    print("="*70)

    return spark


def save_results(df, analysis_results, output_path: str = "output"):
    """
    Saves analysis results

    Args:
        df: Processed DataFrame
        analysis_results: Analysis results
        output_path: Path to save outputs
    """
    print("\n" + "="*70)
    print(" Saving Analysis Results")
    print("="*70)

    os.makedirs(output_path, exist_ok=True)

    # Saves processed dataset
    print("\nSaving processed dataset...")
    df.write.mode('overwrite').parquet(os.path.join(output_path, 'processed_data.parquet'))
    print(f"Saved to: {os.path.join(output_path, 'processed_data.parquet')}")

    # Also saves in CSV (sample)
    df.limit(1000).write.mode('overwrite').csv(
        os.path.join(output_path, 'processed_data_sample.csv'),
        header=True
    )
    print(f"    Sample saved to: {os.path.join(output_path, 'processed_data_sample.csv')}")

    # Saves analyses
    print("\nSaving analysis results...")

    if 'yearly_analysis' in analysis_results:
        analysis_results['yearly_analysis'].write.mode('overwrite').csv(
            os.path.join(output_path, 'yearly_analysis.csv'),
            header=True
        )
        print(f"    Yearly analysis saved")

    if 'monthly_analysis' in analysis_results:
        analysis_results['monthly_analysis'].write.mode('overwrite').csv(
            os.path.join(output_path, 'monthly_analysis.csv'),
            header=True
        )
        print(f"    Monthly analysis saved")

    if 'quarterly_analysis' in analysis_results:
        analysis_results['quarterly_analysis'].write.mode('overwrite').csv(
            os.path.join(output_path, 'quarterly_analysis.csv'),
            header=True
        )
        print(f"    Quarterly analysis saved")

    if 'weekday_analysis' in analysis_results:
        analysis_results['weekday_analysis'].write.mode('overwrite').csv(
            os.path.join(output_path, 'weekday_analysis.csv'),
            header=True
        )
        print(f"    Weekday analysis saved")

    print("\n All results saved successfully!")
    print("="*70)


def main():
    """Main pipeline function"""

    global spark
    print("\n" + "="*70)
    print("NVIDIA STOCK ANALYSIS PIPELINE")
    print("="*70)
    print("\nPipeline Steps:")
    print("1. Initialize Spark Session")
    print("2. Load Data")
    print("3. Process Data")
    print("4. Engineer Features")
    print("5. Run Analysis")
    print("6. Create Visualizations")
    print("7. Save Results")
    print("="*70)

    try:
        # 1. Create Spark Session
        spark = create_spark_session()

        # 2. Load data
        print("\n" + "="*70)
        print("STEP 1: DATA LOADING")
        print("="*70)

        loader = DataLoader(spark)
        df_raw = loader.load_data()
        loader.validate_data(df_raw)
        loader.get_data_summary(df_raw)

        # 3. Process data
        print("\n" + "="*70)
        print("STEP 2: DATA PROCESSING")
        print("="*70)

        processor = DataProcessor()
        df_processed = processor.process_pipeline(df_raw)

        date_range = processor.get_date_range(df_processed)
        print(f"\nDate Range: {date_range['min_date']} to {date_range['max_date']}")

        # 4. Feature Engineering
        print("\n" + "="*70)
        print("STEP 3: FEATURE ENGINEERING")
        print("="*70)

        engineer = FeatureEngineer()
        df_features = engineer.engineer_features(df_processed)

        # Cache DataFrame for better performance
        df_features.cache()

        print(f"\nFinal Dataset Shape:")
        print(f"   Rows: {df_features.count():,}")
        print(f"   Columns: {len(df_features.columns)}")

        # 5. Analyses
        print("\n" + "="*70)
        print("STEP 4: COMPREHENSIVE ANALYSIS")
        print("="*70)

        # analyzer = StockAnalyzer()
        analysis_results = run_comprehensive_analysis(df_features)

        # 6. Visualizations
        print("\n" + "="*70)
        print("STEP 5: VISUALIZATIONS")
        print("="*70)

        visualizer = Visualizer()
        visualizer.create_all_visualizations(df_features, analysis_results)

        # 7. Save results
        save_results(df_features, analysis_results)

        # Displays final summary
        print("\n" + "="*70)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*70)
        print("\nSummary:")
        print(f"   Total Records Processed: {df_features.count():,}")
        print(f"   Features Created: {len(df_features.columns)}")
        print(f"   Date Range: {date_range['min_date']} to {date_range['max_date']}")
        print(f"\nOutput Location: ./output/")
        print(f"   - Processed Data: output/processed_data.parquet")
        print(f"   - Visualizations: output/visualizations/")
        print(f"   - Analysis Results: output/*.csv")
        print("\n" + "="*70)

        # Shows some final statistics
        print("\nKey Insights:")
        overall = analysis_results['overall_stats']
        risk = analysis_results['risk_metrics']
        signals = analysis_results['technical_signals']

        print(f"\nPrice Statistics:")
        print(f"   Current Price: ${signals['close_price']:.2f}")
        print(f"   All-Time High: ${overall['max_price']:.2f}")
        print(f"   All-Time Low: ${overall['min_price']:.2f}")
        print(f"   Average Price: ${overall['avg_price']:.2f}")

        print(f"\nRisk Metrics:")
        print(f"   Daily Volatility: {risk['volatility']:.4f}%")
        print(f"   Sharpe Ratio: {risk['sharpe_ratio']:.4f}" if risk['sharpe_ratio'] else "   Sharpe Ratio: N/A")
        print(f"   Max Daily Loss: {risk['max_loss']:.2f}%")
        print(f"   Max Daily Gain: {risk['max_gain']:.2f}%")

        print(f"\nCurrent Technical Signals:")
        if signals.get('rsi'):
            print(f"   RSI: {signals['rsi']:.2f} - {signals.get('rsi_signal', 'N/A')}")
        if signals.get('macd_signal_interpretation'):
            print(f"   MACD: {signals['macd_signal_interpretation']}")

        print("\n" + "="*70)
        print("Analysis pipeline completed successfully!")
        print("   Check the output/ directory for all results and visualizations.")
        print("="*70 + "\n")

    except Exception as e:
        print(f"\nError in pipeline: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # Stops Spark Session
        if 'spark' in locals():
            spark.stop()
            print("\nSpark Session stopped")


if __name__ == "__main__":
    main()