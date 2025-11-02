"""
Credit Score Comparison Pipeline - Main Airflow DAG

This DAG orchestrates the end-to-end pipeline for comparing FICO and VantageScore 4.0
credit scores using Fannie Mae data.

Author: Isaac Vergara
Date: 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import sys
import os

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import configuration
from config.pipeline_config import (
    DAG_CONFIG, 
    TASK_CONFIG,
    VANTAGESCORE_RAW,
    LOAN_DATA_RAW,
    VANTAGESCORE_CLEAN,
    LOAN_DATA_CLEAN,
    MERGED_DATA,
    DB_TABLE_NAME,
    VIZ_DIR,
    MODEL_DIR
)

# Default arguments for all tasks
default_args = {
    'owner': 'isaac_vergara',
    'depends_on_past': False,
    'start_date': datetime.strptime(DAG_CONFIG['start_date_str'], '%Y-%m-%d'),
    'email_on_failure': TASK_CONFIG['email_on_failure'],
    'email_on_retry': TASK_CONFIG['email_on_retry'],
    'retries': TASK_CONFIG['retries'],
    'retry_delay': timedelta(seconds=TASK_CONFIG['retry_delay_seconds']),
    'execution_timeout': timedelta(seconds=TASK_CONFIG['execution_timeout_seconds'])
}

# Create the DAG
dag = DAG(
    dag_id=DAG_CONFIG['dag_id'],
    default_args=default_args,
    description='Compare FICO vs VantageScore 4.0 using Fannie Mae loan data',
    schedule_interval=DAG_CONFIG['schedule_interval'],
    start_date=datetime.strptime(DAG_CONFIG['start_date_str'], '%Y-%m-%d'),
    catchup=DAG_CONFIG['catchup'],
    max_active_runs=DAG_CONFIG['max_active_runs'],
    tags=DAG_CONFIG['tags']
)


# ============================================================================
# TASK FUNCTIONS (Imports from scripts)
# ============================================================================

def download_vantagescore_data(**kwargs):
    """Download VantageScore 4.0 historical data from Fannie Mae"""
    from scripts.ingestion.download_vantagescore import download_vantagescore
    
    output_path = kwargs.get('output_path', VANTAGESCORE_RAW)
    result = download_vantagescore(output_path)
    
    # Push file path to XCom (not the data itself!)
    kwargs['ti'].xcom_push(key='vantagescore_file', value=output_path)
    return output_path


def download_loan_data(**kwargs):
    """Download Fannie Mae loan acquisition data"""
    from scripts.ingestion.download_fannie_mae import download_fannie_mae
    
    output_path = kwargs.get('output_path', LOAN_DATA_RAW)
    result = download_fannie_mae(output_path)
    
    # Push file path to XCom
    kwargs['ti'].xcom_push(key='loan_file', value=output_path)
    return output_path


def transform_vantagescore(**kwargs):
    """Clean and transform VantageScore data"""
    from scripts.transformation.transform_vantagescore import transform_vantagescore_data
    
    input_path = kwargs.get('input_path', VANTAGESCORE_RAW)
    output_path = kwargs.get('output_path', VANTAGESCORE_CLEAN)
    
    result = transform_vantagescore_data(input_path, output_path)
    
    # Push cleaned file path to XCom
    kwargs['ti'].xcom_push(key='vantagescore_clean', value=output_path)
    return output_path


def transform_loan(**kwargs):
    """Clean and transform loan data"""
    from scripts.transformation.transform_loans import transform_loan_data
    
    input_path = kwargs.get('input_path', LOAN_DATA_RAW)
    output_path = kwargs.get('output_path', LOAN_DATA_CLEAN)
    
    result = transform_loan_data(input_path, output_path)
    
    # Push cleaned file path to XCom
    kwargs['ti'].xcom_push(key='loan_clean', value=output_path)
    return output_path


def merge_datasets(**kwargs):
    """Merge VantageScore and loan data on loan_identifier"""
    from scripts.transformation.merge_datasets import merge_credit_scores
    
    vs_path = kwargs.get('vs_path', VANTAGESCORE_CLEAN)
    loan_path = kwargs.get('loan_path', LOAN_DATA_CLEAN)
    output_path = kwargs.get('output_path', MERGED_DATA)
    
    result = merge_credit_scores(vs_path, loan_path, output_path)
    
    # Push merged file path to XCom
    kwargs['ti'].xcom_push(key='merged_data', value=output_path)
    return output_path


def load_to_postgres(**kwargs):
    """Load merged data to PostgreSQL in chunks"""
    from scripts.loading.load_to_postgres import load_parquet_to_postgres_chunked
    
    input_path = kwargs.get('input_path', MERGED_DATA)
    table_name = kwargs.get('table_name', DB_TABLE_NAME)
    
    rows_loaded = load_parquet_to_postgres_chunked(
        parquet_path=input_path,
        table_name=table_name,
        chunk_size=100000,
        if_exists='replace'  # First load replaces table
    )
    
    return f"Loaded {rows_loaded} rows to {table_name}"


def create_visualizations(**kwargs):
    """Generate comparison visualizations"""
    from scripts.analysis.create_visualizations import generate_visualizations
    
    table_name = kwargs.get('table_name', DB_TABLE_NAME)
    output_dir = kwargs.get('output_dir', VIZ_DIR)
    
    viz_files = generate_visualizations(table_name, output_dir)
    
    return f"Created {len(viz_files)} visualizations"


def train_ml_model(**kwargs):
    """Train machine learning model"""
    from scripts.analysis.train_model import train_comparison_model
    
    table_name = kwargs.get('table_name', DB_TABLE_NAME)
    model_path = kwargs.get('model_path', f"{MODEL_DIR}/credit_model.pkl")
    
    metrics = train_comparison_model(table_name, model_path)
    
    return f"Model trained with AUC: {metrics.get('auc', 'N/A')}"


# ============================================================================
# DAG TASKS DEFINITION
# ============================================================================

# STEP 0: Create database schema
create_schema = PostgresOperator(
    task_id='create_database_schema',
    postgres_conn_id='postgres_default',
    sql='sql/create_tables.sql',
    dag=dag
)

# STEP 1: Data Ingestion (PARALLEL)
with TaskGroup('data_ingestion', dag=dag) as ingestion_group:
    
    download_vs = PythonOperator(
        task_id='download_vantagescore',
        python_callable=download_vantagescore_data,
        op_kwargs={'output_path': VANTAGESCORE_RAW},
        dag=dag
    )
    
    download_loans = PythonOperator(
        task_id='download_loans',
        python_callable=download_loan_data,
        op_kwargs={'output_path': LOAN_DATA_RAW},
        dag=dag
    )

# STEP 2: Data Transformation (PARALLEL)
with TaskGroup('data_transformation', dag=dag) as transformation_group:
    
    transform_vs = PythonOperator(
        task_id='transform_vantagescore',
        python_callable=transform_vantagescore,
        op_kwargs={
            'input_path': VANTAGESCORE_RAW,
            'output_path': VANTAGESCORE_CLEAN
        },
        dag=dag
    )
    
    transform_loan_task = PythonOperator(
        task_id='transform_loans',
        python_callable=transform_loan,
        op_kwargs={
            'input_path': LOAN_DATA_RAW,
            'output_path': LOAN_DATA_CLEAN
        },
        dag=dag
    )

# STEP 3: Merge Datasets
merge_data = PythonOperator(
    task_id='merge_datasets',
    python_callable=merge_datasets,
    op_kwargs={
        'vs_path': VANTAGESCORE_CLEAN,
        'loan_path': LOAN_DATA_CLEAN,
        'output_path': MERGED_DATA
    },
    dag=dag
)

# STEP 4: Load to PostgreSQL
load_data = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgres,
    op_kwargs={
        'input_path': MERGED_DATA,
        'table_name': DB_TABLE_NAME
    },
    dag=dag
)

# STEP 5: Create Indexes for Performance
create_indexes = PostgresOperator(
    task_id='create_indexes',
    postgres_conn_id='postgres_default',
    sql=f"""
        CREATE INDEX IF NOT EXISTS idx_fico ON {DB_TABLE_NAME}(fico_score);
        CREATE INDEX IF NOT EXISTS idx_vs4 ON {DB_TABLE_NAME}(vs4_trimerge);
        CREATE INDEX IF NOT EXISTS idx_quarter ON {DB_TABLE_NAME}(acquisition_quarter);
    """,
    dag=dag
)

# STEP 6: Analysis Tasks (PARALLEL)
with TaskGroup('analysis', dag=dag) as analysis_group:
    
    create_viz = PythonOperator(
        task_id='create_visualizations',
        python_callable=create_visualizations,
        op_kwargs={
            'table_name': DB_TABLE_NAME,
            'output_dir': VIZ_DIR
        },
        dag=dag
    )
    
    train_model = PythonOperator(
        task_id='train_ml_model',
        python_callable=train_ml_model,
        op_kwargs={
            'table_name': DB_TABLE_NAME,
            'model_path': f"{MODEL_DIR}/credit_model.pkl"
        },
        dag=dag
    )

# STEP 7: Cleanup Intermediate Files
cleanup_intermediate = BashOperator(
    task_id='cleanup_intermediate_files',
    bash_command=f'rm -f {VANTAGESCORE_RAW} {LOAN_DATA_RAW} {VANTAGESCORE_CLEAN} {LOAN_DATA_CLEAN}',
    dag=dag
)


# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# Sequential flow with parallel execution where possible
create_schema >> ingestion_group >> transformation_group >> merge_data >> load_data >> create_indexes >> analysis_group >> cleanup_intermediate

# Note: Parallelism happens automatically within TaskGroups:
# - download_vs and download_loans run in parallel
# - transform_vs and transform_loan_task run in parallel  
# - create_viz and train_model run in parallel
