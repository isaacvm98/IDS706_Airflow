def cleanup_intermediate_files(**kwargs):
    """
    Clean up intermediate files
    """
    import os
    
    print("\nCleaning up intermediate files...")
    
    files_to_remove = [
        "/opt/airflow/data/raw/CIRT_Oct25.csv",
    ]
    
    total_freed = 0
    for filepath in files_to_remove:
        if os.path.exists(filepath):
            size = os.path.getsize(filepath) / (1024**2)  # MB
            os.remove(filepath)
            total_freed += size
            print(f"✓ Removed: {filepath} ({size:.1f} MB)")

    return total_freed