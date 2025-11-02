"""
Data Transformation Functions
PySpark-based merge of CIRT and VantageScore data
"""

from pyspark.sql import SparkSession
from pathlib import Path
import pandas as pd


def merge_cirt_vantagescore(cirt_path, vantage_path, glossary_path, output_path):
    """
    Merge CIRT and VantageScore data using PySpark
    
    Args:
        cirt_path: Path to CIRT CSV file
        vantage_path: Path to VantageScore txt file
        glossary_path: Path to glossary Excel file
        output_path: Path to save merged parquet file
    
    Returns:
        int: Number of rows in merged dataset
    """

    
    # Initialize Spark Session
    print("\nInitializing Spark Session...")
    spark = SparkSession.builder \
        .appName("CIRT_VantageScore_Merge") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print(f"✓ Spark {spark.version} initialized")
    print(f"  Master: {spark.sparkContext.master}")
    
    # Read CIRT data
    print(f"\nReading CIRT data from {cirt_path}...")
    cirt_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", "|") \
        .csv(cirt_path)
    
    # Apply glossary column names
    print(f"\nApplying column names from glossary...")
    glossary = pd.read_excel(glossary_path)
    proper_columns = glossary['Field Name'].values.tolist()
    
    old_columns = cirt_df.columns
    for old_col, new_col in zip(old_columns, proper_columns):
        cirt_df = cirt_df.withColumnRenamed(old_col, new_col)
        
    cirt_count = cirt_df.count()
    print(f"✓ Loaded {cirt_count:,} CIRT rows")
    print(f"  Columns: {len(cirt_df.columns)}")
    print(f"  Partitions: {cirt_df.rdd.getNumPartitions()}")
    
    # Read VantageScore data
    print(f"\nReading VantageScore data from {vantage_path}...")
    vantage_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", "|") \
        .csv(vantage_path)
    
    vantage_count = vantage_df.count()
    print(f"✓ Loaded {vantage_count:,} VantageScore rows")
    print(f"  Columns: {len(vantage_df.columns)}")
    
    merged_df = cirt_df.join(
        vantage_df,
        cirt_df["Loan Identifier"] == vantage_df["loan_identifier"],
        how="inner"
    )
    
    # Cache for multiple operations
    merged_df.cache()
    
    merged_count = merged_df.count()
    print(f"✓ Merged dataset: {merged_count:,} rows")
    
    # Count matches
    matches = merged_df.filter(merged_df["loan_identifier"].isNotNull()).count()
    match_rate = (matches / merged_count * 100) if merged_count > 0 else 0
    print(f"  VantageScore matches: {matches:,} ({match_rate:.2f}%)")
    
    # Save as Parquet
    print(f"\nSaving merged data to {output_path}...")
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write as single parquet file
    merged_df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(str(output_dir / "merged_temp"))
    
    # Find the part file and rename it
    import glob
    part_files = glob.glob(str(output_dir / "merged_temp" / "part-*.parquet"))
    if part_files:
        import shutil
        shutil.move(part_files[0], output_path)
        shutil.rmtree(output_dir / "merged_temp")
        print(f"✓ Saved to {output_path}")
    else:
        print(f"✓ Saved to {output_dir / 'merged_temp'} (multiple parquet files)")
    
    # Show sample
    print("\nSample of merged data:")
    merged_df.select(
        "Loan Identifier",
        "Original Interest Rate",
        "Original UPB",
        "vs4_trimerge",
        "vs4_current_method"
    ).show(5, truncate=False)
    
    # Stop Spark
    print("\nStopping Spark Session...")
    spark.stop()
    
    return merged_count