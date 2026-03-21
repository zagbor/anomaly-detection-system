import requests
import json
import time

URL = "http://localhost:5000/api/summary"
TRAFFIC_URL = "http://localhost:5000/api/traffic"

def test_api():
    print(f"Testing API at {URL}...")
    try:
        response = requests.get(URL)
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print("Summary Data:")
            print(f"- Anomalies (24h): {data.get('anomalies_24h')}")
            print(f"- Devices: {data.get('devices_count')}")
            print(f"- Recent Traffic count: {len(data.get('recent_traffic', []))}")
            
            if data.get('recent_traffic'):
                print("\nFirst traffic entry:")
                print(json.dumps(data['recent_traffic'][0], indent=2))
        else:
            print(f"Error: {response.text}")
            
        print(f"\nTesting Traffic API at {TRAFFIC_URL}...")
        response = requests.get(TRAFFIC_URL)
        if response.status_code == 200:
            traffic = response.json()
            print(f"Traffic entries returned: {len(traffic)}")
        else:
            print(f"Error: {response.text}")

    except Exception as e:
        print(f"Connection error: {e}")

if __name__ == "__main__":
    test_api()
