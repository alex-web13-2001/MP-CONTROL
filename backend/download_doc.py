import requests
import json
import os

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzc4OTEwNTQzLCJpZCI6IjAxOWE4MzdjLTI3MGUtNzg2My1iZGM0LWU0YTljMTFmM2EzYyIsImlpZCI6MTMxMjExMTYyLCJvaWQiOjEzNTg2MzAsInMiOjE2MTI2LCJzaWQiOiJiMWMwNWE3YS01NWQwLTQ4YmItOTgzZi05NjM0MTAwZmI2MDIiLCJ0IjpmYWxzZSwidWlkIjoxMzEyMTExNjJ9.D07MKx2fB9t2OefPFWc6B30M9Iut-GWXL695OrHxvrZiXBs0dUPSbBMGKNRIKrCRb_9qb_8DJtvJi7b2ZZ09Yg"
HOST = "https://documents-api.wildberries.ru"
DOC_NAME = "weekly-implementation-report-607996536" 

def main():
    headers = {
        "Authorization": API_KEY,
    }
    
    # Attempt 1: GET /content with params
    url = f"{HOST}/api/v1/documents/content"
    params = {"documentName": DOC_NAME}
    
    print(f"Attempt 1: GET {url} with params={params}")
    try:
        resp = requests.get(url, headers=headers, params=params)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print("Download Success!")
            with open("report.zip", "wb") as f:
                f.write(resp.content)
            print("Saved to report.zip")
            return
        else:
            print(f"Error: {resp.text}")
    except Exception as e:
        print(f"Exception 1: {e}")

    # Attempt 3: GET /download/{name}
    url3 = f"{HOST}/api/v1/documents/download/{DOC_NAME}"
    print(f"\nAttempt 3: GET {url3}")
    try:
        resp3 = requests.get(url3, headers=headers)
        print(f"Status: {resp3.status_code}")
        if resp3.status_code == 200:
            print("Download Success (Method 3)!")
            with open("report.zip", "wb") as f:
                f.write(resp3.content)
            return
        else:
            print(f"Error: {resp3.text}")
    except Exception as e:
        print(f"Exception 3: {e}")

    # Attempt 4: Common API
    HOST_COMMON = "https://common-api.wildberries.ru"
    url4 = f"{HOST_COMMON}/api/v1/documents/download/{DOC_NAME}"
    print(f"\nAttempt 4: GET {url4}")
    try:
        resp4 = requests.get(url4, headers=headers)
        print(f"Status: {resp4.status_code}")
        if resp4.status_code == 200:
             print("Success Method 4")
             with open("report.zip", "wb") as f:
                 f.write(resp4.content)
             return
        else:
             print(f"Error: {resp4.text}")
    except Exception as e:
        print(f"Exception 4: {e}")

    # Attempt 5: GET /documents/download with query param
    # User says: .../api/v1/documents/download/get -> implies GET method on that path
    url5 = f"{HOST}/api/v1/documents/download"
    params = {"documentName": DOC_NAME}
    print(f"\nAttempt 5: GET {url5} with params={params}")
    try:
        resp5 = requests.get(url5, headers=headers, params=params)
        print(f"Status: {resp5.status_code}")
        if resp5.status_code == 200:
             print("Success Method 5")
             with open("report_method5.zip", "wb") as f:
                 f.write(resp5.content)
             return
        else:
             print(f"Error: {resp5.text}")
    except Exception as e:
        print(f"Exception 5: {e}")

    # Attempt 6: Correct Params based on error message
    url6 = f"{HOST}/api/v1/documents/download"
    # Note: 'serviceName' and 'extension' are required.
    # From checking docs: extension is "zip"
    params6 = {
        "serviceName": DOC_NAME, # "weekly-implementation-report-..."
        "extension": "zip"
    }
    print(f"\nAttempt 6: GET {url6} with params={params6}")
    try:
        resp6 = requests.get(url6, headers=headers, params=params6)
        print(f"Status: {resp6.status_code}")
        if resp6.status_code == 200:
             print("Success Method 6")
             with open("report_method6.zip", "wb") as f:
                 f.write(resp6.content)
             return
        else:
             print(f"Error: {resp6.text}")
             
            # Attempt 6b: POST with same params? No, error said "serviceName and extension required" on GET usually.
    except Exception as e:
        print(f"Exception 6: {e}")
        
if __name__ == "__main__":
    main()
