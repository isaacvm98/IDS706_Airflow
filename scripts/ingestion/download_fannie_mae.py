"""
Data Ingestion Functions
Download data from Fannie Mae API
"""

import requests
import zipfile
from io import BytesIO
from pathlib import Path
import os

BASE_64_AUTH = os.environ['BASE_64_AUTH']

def download_cirt(output_path):
    """
    Download CIRT data from Fannie Mae API
    
    Args:
        output_path: Path to save CIRT CSV file
    
    Returns:
        str: Path to downloaded file
    """
    # Fannie Mae API credentials
    AUTH_URL = "https://auth.pingone.com/4c2b23f9-52b1-4f8f-aa1f-1d477590770c/as/token"
    DATA_URL = "https://api.fanniemae.com/v1/credit-insurance-risk-transfer/current-reporting-period"
    
    print("Step 1: Getting access token...")
    headers = {
        "Authorization": f"Basic {BASE_64_AUTH}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    response = requests.post(AUTH_URL, headers=headers, 
                            data={"grant_type": "client_credentials"})
    access_token = response.json()["access_token"]
    print("✓ Token obtained")
    
    print("Step 2: Requesting data...")
    headers = {
        "x-public-access-token": access_token,
        "Content-Type": "application/json"
    }
    
    response = requests.get(DATA_URL, headers=headers)
    s3_uri = response.json()['s3Uri']
    print(f"✓ Data URL: {s3_uri}")
    
    print("Step 3: Downloading and extracting ZIP...")
    zip_response = requests.get(s3_uri)
    
    extract_dir = Path(output_path).parent
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(BytesIO(zip_response.content)) as z:
        print("Files in ZIP:")
        for name in z.namelist():
            print(f"  - {name}")
        
        # Extract all files
        z.extractall(extract_dir)
        
        # Find CIRT file
        cirt_files = [f for f in z.namelist() if f.endswith('.csv') or 'CIRT' in f]
        
        if cirt_files:
            cirt_file = cirt_files[0]
            cirt_path = os.path.join(extract_dir, cirt_file)
            
            # Rename to expected name
            if cirt_path != output_path:
                os.rename(cirt_path, output_path)
            
            print(f"✓ Downloaded CIRT to: {output_path}")
        else:
            print("Available files:")
            for f in z.namelist():
                print(f"  - {f}")
            raise ValueError("Could not find CIRT CSV file in ZIP")
    
    return output_path