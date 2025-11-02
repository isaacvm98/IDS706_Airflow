"""
Download VantageScore Test Data from Fannie Mae API
Simple version - just downloads the small test dataset
"""

import requests
import os
import zipfile
from io import BytesIO
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fannie Mae API credentials
CLIENT_ID = "b847d8cb-7f95-47c4-b547-3512af9cf0e1"
CLIENT_SECRET = "AsebVRBeyGfg2frKwNqDmd_k00KsF1nz27Ngo.oBbjmC7cs8Vsy_NnzeYGieXfhP"
BASE_64_AUTH = "Yjg0N2Q4Y2ItN2Y5NS00N2M0LWI1NDctMzUxMmFmOWNmMGUxOkFzZWJWUkJleUdmZzJmckt3TnFEbWRfazAwS3NGMW56MjdOZ28ub0Jiam1DN2NzOFZzeV9ObnplWUdpZVhmaFA="
AUTH_URL = "https://auth.pingone.com/4c2b23f9-52b1-4f8f-aa1f-1d477590770c/as/token"
DATA_URL = "https://api.fanniemae.com/v1/credit-insurance-risk-transfer/current-reporting-period"


def download_vantagescore(output_path):
    """Download VantageScore test data"""
    
    logger.info("Downloading VantageScore test data...")
    
    # Step 1: Get token
    logger.info("1. Getting access token...")
    headers = {
        "Authorization": f"Basic {BASE_64_AUTH}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    response = requests.post(AUTH_URL, headers=headers, 
                            data={"grant_type": "client_credentials"})
    access_token = response.json()["access_token"]
    logger.info("   ✓ Token obtained")
    
    # Step 2: Get data URL
    logger.info("2. Requesting data...")
    headers = {
        "x-public-access-token": access_token,
        "Content-Type": "application/json"
    }
    
    response = requests.get(DATA_URL, headers=headers)
    s3_uri = response.json()['s3Uri']
    logger.info(f"   ✓ Data URL: {s3_uri}")
    
    # Step 3: Download and extract ZIP
    logger.info("3. Downloading ZIP file...")
    zip_response = requests.get(s3_uri)
    
    extract_dir = os.path.dirname(output_path)
    os.makedirs(extract_dir, exist_ok=True)
    
    with zipfile.ZipFile(BytesIO(zip_response.content)) as z:
        logger.info("   Files in ZIP:")
        for name in z.namelist():
            logger.info(f"     - {name}")
        
        # Extract all
        z.extractall(extract_dir)
        
        # Find VantageScore file
        vs_file = [f for f in z.namelist() if 'VantageScore' in f][0]
        vs_path = os.path.join(extract_dir, vs_file)
        
        # Rename to expected name
        if vs_path != output_path:
            os.rename(vs_path, output_path)
    
    # Quick stats
    with open(output_path, 'r') as f:
        lines = sum(1 for _ in f)
    
    logger.info(f"   ✓ Downloaded: {lines:,} records")
    logger.info(f"   ✓ Saved to: {output_path}")
    
    return output_path


if __name__ == "__main__":
    import sys
    output = sys.argv[1] if len(sys.argv) > 1 else "data/raw/vantagescore_test.txt"
    
    result = download_vantagescore(output)
    print(f"\n✓ Success! {result}")