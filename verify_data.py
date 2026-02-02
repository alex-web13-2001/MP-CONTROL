import requests
import json
from decimal import Decimal

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzc4OTEwNTQzLCJpZCI6IjAxOWE4MzdjLTI3MGUtNzg2My1iZGM0LWU0YTljMTFmM2EzYyIsImlpZCI6MTMxMjExMTYyLCJvaWQiOjEzNTg2MzAsInMiOjE2MTI2LCJzaWQiOiJiMWMwNWE3YS01NWQwLTQ4YmItOTgzZi05NjM0MTAwZmI2MDIiLCJ0IjpmYWxzZSwidWlkIjoxMzEyMTExNjJ9.D07MKx2fB9t2OefPFWc6B30M9Iut-GWXL695OrHxvrZiXBs0dUPSbBMGKNRIKrCRb_9qb_8DJtvJi7b2ZZ09Yg"
URL = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
DATE_FROM = "2025-12-01"
DATE_TO = "2025-12-31"
TARGET_NM_ID = 279167715

def main():
    headers = {"Authorization": API_KEY}
    params = {
        "dateFrom": DATE_FROM,
        "dateTo": DATE_TO,
        "limit": 100000
    }
    
    print(f"Fetching data from {DATE_FROM} to {DATE_TO}...")
    try:
        resp = requests.get(URL, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return
    
    print(f"Total rows fetched: {len(data)}")
    
    # Filter and Aggregate
    stats = {
        "count_rows": 0,
        "quantity": 0,
        "retail_amount": Decimal(0),
        "commission_amount": Decimal(0),
        "logistics": Decimal(0),
        "penalty": Decimal(0),
        "payout": Decimal(0),
        "storage": Decimal(0),
        "acceptance": Decimal(0)
    }
    
    for row in data:
        if row.get("nm_id") != TARGET_NM_ID:
            continue
            
        stats["count_rows"] += 1
        stats["quantity"] += row.get("quantity", 0)
        stats["retail_amount"] += Decimal(str(row.get("retail_amount", 0)))
        
        # Commission logic matching parser
        # WB returns ppvz_sales_commission usually.
        stats["commission_amount"] += abs(Decimal(str(row.get("ppvz_sales_commission", 0))))
        
        # Logistics
        deliv = Decimal(str(row.get("delivery_rub", 0)))
        rebill = Decimal(str(row.get("rebill_logistic_cost", 0)))
        stats["logistics"] += (deliv + rebill)
        
        # Penalty
        stats["penalty"] += abs(Decimal(str(row.get("penalty", 0))))
        
        # Payout
        stats["payout"] += Decimal(str(row.get("ppvz_for_pay", 0)))

        # Storage/Acceptance
        stats["storage"] += Decimal(str(row.get("storage_fee", 0)))
        stats["acceptance"] += Decimal(str(row.get("acceptance", 0)))

    print("-" * 30)
    print(f"VERIFICATION RESULTS FOR NM_ID: {TARGET_NM_ID}")
    print("-" * 30)
    print(f"Rows found:       {stats['count_rows']}")
    print(f"Items Sold:       {stats['quantity']}")
    print(f"Sales (Retail):   {stats['retail_amount']}")
    print(f"Payout (To You):  {stats['payout']}")
    print(f"Commission:       {stats['commission_amount']}")
    print(f"Logistics:        {stats['logistics']}")
    print(f"Penalty:          {stats['penalty']}")
    print(f"Storage:          {stats['storage']}")
    print("-" * 30)

if __name__ == "__main__":
    main()
