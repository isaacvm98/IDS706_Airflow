
def upload_to_postgres(parquet_path):
    """Upload merged parquet data to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import create_engine
    
    # Database connection
    db_url = "postgresql://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(db_url)
    
    print(f"\nReading merged data from {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    print(f"✓ Loaded {len(df):,} rows, {len(df.columns)} columns")
    
    # Upload to PostgreSQL
    table_name = "cirt_vantagescore_merged"
    print(f"\nUploading to PostgreSQL table: {table_name}")
    
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',
        index=False,
        chunksize=10000,
        method='multi'
    )
    
    print(f"✓ Uploaded {len(df):,} rows to {table_name}")
    
    # Verify
    with engine.connect() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = result.fetchone()[0]
        print(f"✓ Verified: {count:,} rows in database")
    
    return count