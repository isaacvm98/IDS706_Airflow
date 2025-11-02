"""
Configuration file for Credit Score Comparison Pipeline
Centralizes all configuration parameters
"""

import os
from pathlib import Path

# Base Directories
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data'

# Data Subdirectories
RAW_DATA_DIR = os.getenv('RAW_DATA_DIR', str(DATA_DIR / 'raw'))
PROCESSED_DATA_DIR = os.getenv('PROCESSED_DATA_DIR', str(DATA_DIR / 'processed'))
FINAL_DATA_DIR = os.getenv('FINAL_DATA_DIR', str(DATA_DIR / 'final'))
OUTPUT_DIR = os.getenv('OUTPUT_DIR', str(DATA_DIR / 'outputs'))
VIZ_DIR = f"{OUTPUT_DIR}/visualizations"
MODEL_DIR = f"{OUTPUT_DIR}/models"

# Ensure directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, FINAL_DATA_DIR, OUTPUT_DIR, VIZ_DIR, MODEL_DIR]:
    Path(directory).mkdir(parents=True, exist_ok=True)

# File Paths
VANTAGESCORE_RAW = f"{RAW_DATA_DIR}/VantageScore4_HistoricalScores_CRT.txt" 
LOAN_DATA_RAW = f"{RAW_DATA_DIR}/fannie_mae_acquisitions.csv"
VANTAGESCORE_CLEAN = f"{PROCESSED_DATA_DIR}/vantagescore_clean.parquet"
LOAN_DATA_CLEAN = f"{PROCESSED_DATA_DIR}/loans_clean.parquet"
MERGED_DATA = f"{FINAL_DATA_DIR}/credit_comparison.parquet"

# Database Configuration
DB_CONFIG = {
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'credit_scoring_db')
}

DB_CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

DB_TABLE_NAME = 'credit_score_comparison'

# Processing Configuration
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 100000))
SAMPLE_SIZE = int(os.getenv('SAMPLE_SIZE', 1000000))  # For visualizations

# Data Source URLs (Update these with actual Fannie Mae URLs)
VANTAGESCORE_URL = "https://capitalmarkets.fanniemae.com/..." # Update with actual URL
LOAN_DATA_URL = "https://capitalmarkets.fanniemae.com/..."    # Update with actual URL

# Column Definitions
# Column Definitions
VANTAGESCORE_COLUMNS = [
    'deal_name',           # Changed from acquisition_quarter
    'loan_identifier',
    'vs4_current_method',
    'vs4_trimerge',
    'vs4_bimerge_lowest',
    'vs4_bimerge_median',
    'vs4_bimerge_highest'
]

LOAN_DATA_COLUMNS = [
    'loan_identifier',
    'fico_score',
    'original_ltv',
    'original_dti',
    'original_upb',
    'original_interest_rate',
    'loan_purpose',
    'property_type',
    'number_of_units',
    'occupancy_status',
    'first_time_homebuyer_flag'
]

# Feature Engineering
DERIVED_FEATURES = [
    'score_difference',      # vs4_trimerge - fico_score
    'score_ratio',           # vs4_trimerge / fico_score
    'score_abs_diff',        # abs(score_difference)
    'high_ltv_flag',         # ltv > 80
    'high_dti_flag'          # dti > 43
]

# Machine Learning Configuration
ML_CONFIG = {
    'test_size': 0.2,
    'random_state': 42,
    'cv_folds': 5,
    'model_type': 'random_forest',  # or 'logistic_regression'
    'n_estimators': 100  # for random forest
}

# Visualization Configuration
VIZ_CONFIG = {
    'figure_size': (14, 8),
    'dpi': 300,
    'style': 'seaborn-v0_8',
    'color_palette': 'Set2'
}

# PySpark Configuration (Super Bonus)
SPARK_CONFIG = {
    'app_name': 'CreditScoreTransform',
    'driver_memory': '4g',
    'executor_memory': '4g',
    'master': os.getenv('SPARK_MASTER', 'local[*]')
}

# Data Quality Thresholds
DATA_QUALITY = {
    'min_fico_score': 300,
    'max_fico_score': 850,
    'min_vs4_score': 300,
    'max_vs4_score': 850,
    'max_ltv': 100,
    'max_dti': 100,
    'max_missing_rate': 0.3  # 30% missing values allowed
}

# Airflow DAG Configuration
DAG_CONFIG = {
    'dag_id': 'credit_score_comparison_pipeline',
    'schedule_interval': '@once', 
    'start_date_str': '2025-11-01',
    'catchup': False,
    'max_active_runs': 1,
    'default_view': 'graph',
    'tags': ['credit_scoring', 'risk_management', 'fannie_mae']
}

# Task Configuration
TASK_CONFIG = {
    'retries': 2,
    'retry_delay_seconds': 300,  # 5 minutes
    'execution_timeout_seconds': 3600,  # 1 hour max per task
    'email_on_failure': False,
    'email_on_retry': False
}

print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
print(f"Data directories: {RAW_DATA_DIR}, {PROCESSED_DATA_DIR}, {FINAL_DATA_DIR}")