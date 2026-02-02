import requests
import json

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzc4OTEwNTQzLCJpZCI6IjAxOWE4MzdjLTI3MGUtNzg2My1iZGM0LWU0YTljMTFmM2EzYyIsImlpZCI6MTMxMjExMTYyLCJvaWQiOjEzNTg2MzAsInMiOjE2MTI2LCJzaWQiOiJiMWMwNWE3YS01NWQwLTQ4YmItOTgzZi05NjM0MTAwZmI2MDIiLCJ0IjpmYWxzZSwidWlkIjoxMzEyMTExNjJ9.D07MKx2fB9t2OefPFWc6B30M9Iut-GWXL695OrHxvrZiXBs0dUPSbBMGKNRIKrCRb_9qb_8DJtvJi7b2ZZ09Yg"

# Try multiple hosts
HOSTS = [
    "https://documents-api.wildberries.ru", 
    "https://statistics-api.wildberries.ru",
    "https://common-api.wildberries.ru" 
]

def main():
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }
    
    # Limit must be <= 50 according to error
    params = {
        "dateFrom": "2025-01-01",
        "dateTo": "2026-02-02",
        "limit": 50
    }
    
    # We found the correct host: documents-api
    host = "https://documents-api.wildberries.ru"
    url = f"{host}/api/v1/documents/list"
    
    print(f"Requesting {url} with limit=50...")
    try:
        resp = requests.get(url, headers=headers, params=params)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            data = resp.json()
            # print(data) # Debug
            
            # According to logs, data['data'] is a dict or list?
            # If it is a dict, it might have 'documents' key.
            raw_data = data.get('data')
            if isinstance(raw_data, dict) and 'documents' in raw_data:
                logs = raw_data['documents']
            elif isinstance(raw_data, list):
                logs = raw_data
            else:
                 # Check if the root itself is the list (sometimes happens)
                 logs = raw_data if raw_data else []

            print(f"SUCCESS!")
            print("\n--- FULL DOCUMENT DUMP ---")
            for i, doc in enumerate(logs):
                print(f"--- Doc {i+1} ---")
                print(json.dumps(doc, indent=2, ensure_ascii=False))
                
            print(f"\nTotal: {len(logs)}")
        else:
            print(f"Error: {resp.text}")
            
    except Exception as e:
        print(f"Error: {e}")
        
if __name__ == "__main__":
    main()
