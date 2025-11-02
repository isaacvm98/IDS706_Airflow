"""
VantageScore Data Ingestion - Validates file exists

Since data is already manually downloaded, this script just:
1. Checks file exists
2. Validates format
3. Returns file path
"""

import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_vantagescore(output_path):
    """
    Validate VantageScore file exists and is properly formatted
    
    Args:
        output_path: Expected path to VantageScore file
        
    Returns:
        str: Path to VantageScore file
    """
    logger.info("=" * 60)
    logger.info("VANTAGESCORE DATA VALIDATION")
    logger.info("=" * 60)
    
    try:
        # Check file exists
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"VantageScore file not found: {output_path}")
        
        logger.info(f"✓ File found: {output_path}")
        
        # Get file size
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        logger.info(f"✓ File size: {file_size_mb:.2f} MB")
        
        # Quick validation - check header
        with open(output_path, 'r') as f:
            header = f.readline().strip()
            expected_cols = ['acquisition_quarter', 'loan_identifier', 'vs4_current_method', 
                           'vs4_trimerge', 'vs4_bimerge_lowest', 'vs4_bimerge_median', 
                           'vs4_bimerge_highest']
            
            actual_cols = header.split('|')
            
            if actual_cols != expected_cols:
                logger.warning(f"Header mismatch!")
                logger.warning(f"Expected: {expected_cols}")
                logger.warning(f"Found: {actual_cols}")
            else:
                logger.info("✓ Header validation passed")
            
            # Count lines
            line_count = sum(1 for _ in f) + 1  # +1 for header
            logger.info(f"✓ Total records: {line_count:,}")
        
        # Preview first few lines
        logger.info("\nFile preview (first 3 rows):")
        with open(output_path, 'r') as f:
            for i, line in enumerate(f):
                if i < 4:  # Header + 3 rows
                    logger.info(f"  {line.strip()}")
                else:
                    break
        
        logger.info("=" * 60)
        logger.info("VALIDATION COMPLETE")
        logger.info("=" * 60)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise


if __name__ == "__main__":
    import sys
    
    file_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/VantageScore4_HistoricalScores_CRT.txt"
    
    print(f"\nValidating: {file_path}\n")
    result = download_vantagescore(file_path)
    print(f"\n✓ Success! File validated: {result}")