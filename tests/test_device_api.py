import requests
import json
import time

BASE_URL = "http://localhost:5000/api"

def test_device_api():
    print("=== Testing Device-Specific API Endpoints ===")
    
    # 1. Get all devices to find a valid device_id
    print("\n1. Fetching all devices...")
    try:
        response = requests.get(f"{BASE_URL}/devices")
        if response.status_code != 200:
            print(f"FAILED: Could not fetch devices. Status: {response.status_code}")
            return
        
        devices = response.json()
        if not devices:
            print("WARNING: No devices found in the system. Make sure the detector is running.")
            return
        
        device_id = devices[0]['device_id']
        print(f"Found device: {device_id}")
        
        # 2. Test device traffic endpoint
        print(f"\n2. Fetching traffic for device: {device_id}...")
        traffic_url = f"{BASE_URL}/traffic/{device_id}"
        response = requests.get(traffic_url)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            traffic = response.json()
            print(f"Traffic entries found: {len(traffic)}")
            if traffic:
                print("Sample traffic entry structure:")
                print(json.dumps(traffic[0], indent=2))
        else:
            print(f"FAILED: Traffic API error: {response.text}")
            
        # 3. Test device tags endpoint
        print(f"\n3. Fetching tags for device: {device_id}...")
        tags_url = f"{BASE_URL}/tags/{device_id}"
        response = requests.get(tags_url)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            tags = response.json()
            print(f"Tags found: {len(tags)}")
            if tags:
                print("Sample tag structure:")
                print(json.dumps(tags[0], indent=2))
        else:
            print(f"FAILED: Tags API error: {response.text}")

    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    test_device_api()
