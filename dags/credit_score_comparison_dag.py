"""
PySpark-Based CIRT + VantageScore Merge Pipeline (Modular)

Organized structure with separate modules for each pipeline stage.

Author: Isaac Vergara
Date: 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.ingestion.download_fannie_mae import download_cirt
from scripts.ingestion.merge_data import merge_cirt_vantagescore
from scripts.loading.load_to_postgres import upload_to_postgres
from scripts.modeling.analysis import analyze_credit_scores
from scripts.utils.db_utils import cleanup_intermediate_files

# File paths
RAW_DATA_DIR = "/opt/airflow/data/raw"
FINAL_DATA_DIR = "/opt/airflow/data/final"

CIRT_FILE = f"{RAW_DATA_DIR}/CIRT_Oct25.csv"
VANTAGESCORE_FILE = f"{RAW_DATA_DIR}/VantageScore4_HistoricalScores_CRT.txt"
GLOSSARY_FILE = f"{RAW_DATA_DIR}/crt-file-layout-and-glossary.xlsx"
MERGED_FILE = f"{FINAL_DATA_DIR}/merged_data.parquet"

# Database configuration
DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"
TABLE_NAME = "cirt_vantagescore_merged"

# Default arguments
default_args = {
    'owner': 'isaac_vergara',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    dag_id='pyspark_cirt_vantagescore_merge',
    default_args=default_args,
    description='Download CIRT and merge with VantageScore using PySpark',
    schedule_interval='@once',
    catchup=False,
    tags=['credit_scoring', 'fannie_mae', 'pyspark']
)


# ============================================================================
# TASK WRAPPER FUNCTIONS
# ============================================================================

def download_cirt_task(**kwargs):
    """Wrapper for download_cirt"""
    return download_cirt(CIRT_FILE)


def merge_data_task(**kwargs):
    """Wrapper for merge_cirt_vantagescore"""
    return merge_cirt_vantagescore(
        cirt_path=CIRT_FILE,
        vantage_path=VANTAGESCORE_FILE,
        glossary_path=GLOSSARY_FILE,
        output_path=MERGED_FILE
    )


def upload_postgres_task(**kwargs):
    """Wrapper for upload_to_postgres"""
    return upload_to_postgres(
        parquet_path=MERGED_FILE
    )


def analyze_task(**kwargs):
    """Wrapper for analyze_credit_scores"""
    return analyze_credit_scores(
        db_url=DB_URL,
        table_name=TABLE_NAME
    )


def cleanup_task(**kwargs):
    """Wrapper for cleanup_intermediate_files"""
    return cleanup_intermediate_files(
        files_to_remove=[CIRT_FILE]
    )


# ============================================================================
# DAG TASKS DEFINITION
# ============================================================================

download_cirt_op = PythonOperator(
    task_id='download_cirt',
    python_callable=download_cirt_task,
    dag=dag
)

merge_data_op = PythonOperator(
    task_id='merge_with_pyspark',
    python_callable=merge_data_task,
    dag=dag
)

upload_postgres_op = PythonOperator(
    task_id='upload_to_postgresql',
    python_callable=upload_postgres_task,
    dag=dag
)

analyze_scores_op = PythonOperator(
    task_id='analyze_credit_scores',
    python_callable=analyze_task,
    dag=dag
)

cleanup_files_op = PythonOperator(
    task_id='cleanup_intermediate_files',
    python_callable=cleanup_task,
    dag=dag
)


# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

download_cirt_op >> merge_data_op >> upload_postgres_op >> analyze_scores_op >> cleanup_files_op