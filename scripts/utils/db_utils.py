"""
Database utility functions for PostgreSQL operations
"""

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import NullPool
import logging
from typing import Optional, List
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.config.pipeline_config import DB_CONNECTION_STRING, DB_TABLE_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db_engine():
    """
    Create and return SQLAlchemy engine
    
    Returns:
        sqlalchemy.engine.Engine: Database engine
    """
    try:
        engine = create_engine(
            DB_CONNECTION_STRING,
            poolclass=NullPool,  # Avoid connection pool issues in Airflow
            echo=False
        )
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise


def test_connection():
    """
    Test database connection
    
    Returns:
        bool: True if connection successful
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def table_exists(table_name: str) -> bool:
    """
    Check if table exists in database
    
    Args:
        table_name: Name of table to check
        
    Returns:
        bool: True if table exists
    """
    try:
        engine = get_db_engine()
        inspector = inspect(engine)
        exists = table_name in inspector.get_table_names()
        logger.info(f"Table '{table_name}' exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        return False


def execute_sql_file(sql_file_path: str):
    """
    Execute SQL statements from a file
    
    Args:
        sql_file_path: Path to SQL file
    """
    try:
        with open(sql_file_path, 'r') as f:
            sql_commands = f.read()
        
        engine = get_db_engine()
        with engine.connect() as conn:
            # Split by semicolon and execute each statement
            for statement in sql_commands.split(';'):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))
                    conn.commit()
        
        logger.info(f"Successfully executed SQL file: {sql_file_path}")
    except Exception as e:
        logger.error(f"Error executing SQL file: {e}")
        raise


def load_df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = 'append',
    chunk_size: int = 10000
):
    """
    Load pandas DataFrame to PostgreSQL table
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        if_exists: 'fail', 'replace', or 'append'
        chunk_size: Number of rows per chunk
    """
    try:
        engine = get_db_engine()
        
        rows_loaded = df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=chunk_size
        )
        
        logger.info(f"Loaded {len(df)} rows to table '{table_name}'")
        return rows_loaded
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {e}")
        raise


def load_parquet_to_postgres_chunked(
    parquet_path: str,
    table_name: str,
    chunk_size: int = 100000,
    if_exists: str = 'append'
):
    """
    Load large Parquet file to PostgreSQL in chunks
    
    Args:
        parquet_path: Path to Parquet file
        table_name: Target table name
        chunk_size: Rows per chunk
        if_exists: 'fail', 'replace', or 'append'
    """
    try:
        engine = get_db_engine()
        total_rows = 0
        
        # Read and load in chunks
        for i, chunk in enumerate(pd.read_parquet(parquet_path, engine='pyarrow', 
                                                    chunksize=chunk_size)):
            # First chunk: replace or fail, subsequent: append
            mode = if_exists if i == 0 else 'append'
            
            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists=mode,
                index=False,
                method='multi',
                chunksize=10000
            )
            
            total_rows += len(chunk)
            logger.info(f"Loaded chunk {i+1}: {len(chunk)} rows (Total: {total_rows})")
        
        logger.info(f"Successfully loaded {total_rows} total rows to '{table_name}'")
        return total_rows
    except Exception as e:
        logger.error(f"Error loading Parquet to PostgreSQL: {e}")
        raise


def query_to_df(query: str) -> pd.DataFrame:
    """
    Execute SQL query and return as DataFrame
    
    Args:
        query: SQL query string
        
    Returns:
        pd.DataFrame: Query results
    """
    try:
        engine = get_db_engine()
        df = pd.read_sql(query, con=engine)
        logger.info(f"Query returned {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def get_table_row_count(table_name: str) -> int:
    """
    Get number of rows in table
    
    Args:
        table_name: Name of table
        
    Returns:
        int: Row count
    """
    try:
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        df = query_to_df(query)
        count = int(df['count'].iloc[0])
        logger.info(f"Table '{table_name}' has {count:,} rows")
        return count
    except Exception as e:
        logger.error(f"Error getting row count: {e}")
        return 0


def create_indexes(table_name: str, columns: List[str]):
    """
    Create indexes on specified columns
    
    Args:
        table_name: Table name
        columns: List of column names to index
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            for col in columns:
                index_name = f"idx_{table_name}_{col}"
                sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})"
                conn.execute(text(sql))
                conn.commit()
                logger.info(f"Created index: {index_name}")
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")
        raise


def drop_table(table_name: str):
    """
    Drop table if exists
    
    Args:
        table_name: Name of table to drop
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            conn.commit()
        logger.info(f"Dropped table: {table_name}")
    except Exception as e:
        logger.error(f"Error dropping table: {e}")
        raise


def get_table_sample(table_name: str, n: int = 1000) -> pd.DataFrame:
    """
    Get random sample from table
    
    Args:
        table_name: Table name
        n: Number of rows to sample
        
    Returns:
        pd.DataFrame: Sample data
    """
    try:
        # Use TABLESAMPLE for large tables (PostgreSQL 9.5+)
        query = f"""
            SELECT * FROM {table_name}
            TABLESAMPLE BERNOULLI ({(n/get_table_row_count(table_name))*100})
            LIMIT {n}
        """
        return query_to_df(query)
    except Exception as e:
        logger.error(f"Error sampling table: {e}")
        # Fallback to ORDER BY RANDOM()
        query = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT {n}"
        return query_to_df(query)


if __name__ == "__main__":
    # Test connection
    print("Testing database connection...")
    if test_connection():
        print("✓ Database connection successful")
    else:
        print("✗ Database connection failed")