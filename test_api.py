"""Quick test script for bulk import API."""
import requests
import time

BASE_URL = "http://127.0.0.1:8001/api/v1/bulk-import"

def test_performance():
    """Test performance endpoint."""
    print("🧪 Testing /performance endpoint...")
    response = requests.get(f"{BASE_URL}/performance")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print()

def test_list_jobs():
    """Test jobs list endpoint."""
    print("🧪 Testing /jobs endpoint...")
    response = requests.get(f"{BASE_URL}/jobs")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print()

def test_import_parquet():
    """Test parquet import endpoint."""
    print("🧪 Testing /parquet endpoint...")
    response = requests.post(
        f"{BASE_URL}/parquet",
        json={
            "parquet_urls": [
                "https://eeadmz1-downloads-webapp.azurewebsites.net/api/parquet/IT_5_20230101010000_20230101020000.parquet"
            ],
            "max_workers": 4,
            "batch_size": 50000
        }
    )
    print(f"Status: {response.status_code}")
    data = response.json()
    print(f"Job ID: {data.get('job_id')}")
    print(f"Status: {data.get('status')}")
    print()
    
    # Wait and check status
    job_id = data.get("job_id")
    if job_id:
        print("⏳ Waiting 5 seconds...")
        time.sleep(5)
        
        print(f"🧪 Checking job status for {job_id}...")
        response = requests.get(f"{BASE_URL}/jobs/{job_id}")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")


if __name__ == "__main__":
    print("=" * 60)
    print("Bulk Import API Test")
    print("=" * 60)
    print()
    
    try:
        test_performance()
        test_list_jobs()
        test_import_parquet()
        
        print("✅ All tests completed!")
    except Exception as e:
        print(f"❌ Error: {e}")
