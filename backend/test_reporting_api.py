import requests
import json
import time

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzc4OTEwNTQzLCJpZCI6IjAxOWE4MzdjLTI3MGUtNzg2My1iZGM0LWU0YTljMTFmM2EzYyIsImlpZCI6MTMxMjExMTYyLCJvaWQiOjEzNTg2MzAsInMiOjE2MTI2LCJzaWQiOiJiMWMwNWE3YS01NWQwLTQ4YmItOTgzZi05NjM0MTAwZmI2MDIiLCJ0IjpmYWxzZSwidWlkIjoxMzEyMTExNjJ9.D07MKx2fB9t2OefPFWc6B30M9Iut-GWXL695OrHxvrZiXBs0dUPSbBMGKNRIKrCRb_9qb_8DJtvJi7b2ZZ09Yg"

def main():
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }

    # Step 1 Failed. Skipping to Step 2 with KNOWN ID from documents/list
    # Document Name: "Отчет №607996536..." -> ID: 607996536
    report_id = 607996536
    print(f"Skipping Step 1. Trying Step 2 with Hardcoded ID: {report_id}")

    # Try Generate on Common API
    url_gen = "https://common-api.wildberries.ru/api/v1/reports/financial/generate"
    print(f"\nStep 2: Requesting Generation: {url_gen}")
    
    # Try with Integer ID
    gen_body = {"id": report_id}
    try:
        resp_gen = requests.post(url_gen, headers=headers, json=gen_body)
        print(f"Status (Int ID): {resp_gen.status_code}")
        print("Response:", resp_gen.text)
        
        if resp_gen.status_code == 200:
             print("SUCCESS! Generation started.")
        elif resp_gen.status_code == 404:
             # Try Statistics API
             url_gen_stat = "https://statistics-api.wildberries.ru/api/v1/reports/financial/generate"
             print(f"\nRetrying Step 2 on Statistics API: {url_gen_stat}")
             resp_gen2 = requests.post(url_gen_stat, headers=headers, json=gen_body)
             print(f"Status: {resp_gen2.status_code}")
             print("Response:", resp_gen2.text)

    except Exception as e:
        print(f"Gen Error: {e}")
        
    # Try with String ServiceName?
    gen_body_str = {"id": "weekly-implementation-report-607996536"}
    # Pass for now unless requested

if __name__ == "__main__":
    main()
