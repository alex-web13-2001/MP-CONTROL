
import json
from decimal import Decimal

def parse_full_stats_v3(full_stats):
    total_views = 0
    total_clicks = 0
    rows_count = 0
    nm_views = {}

    for campaign in full_stats:
        advert_id = int(campaign.get("advertId", 0))
        days = campaign.get("days", [])
        
        print(f"Campaign {advert_id}: {len(days)} days")
        
        for d in days:
            apps = d.get("apps", [])
            for app in apps:
                nms_list = app.get("nms", [])
                for nm in nms_list:
                    nm_id = int(nm.get("nmId", 0))
                    views = int(nm.get("views", 0))
                    
                    total_views += views
                    total_clicks += int(nm.get("clicks", 0))
                    rows_count += 1
                    
                    nm_views[nm_id] = nm_views.get(nm_id, 0) + views

    print(f"Total Parsed Views: {total_views}")
    print(f"Total Parsed Clicks: {total_clicks}")
    print(f"Rows count: {rows_count}")
    print("Views by NM:")
    for nm, v in nm_views.items():
        print(f"  NM {nm}: {v}")

with open("stats_debug_25690526.json", "r") as f:
    data = json.load(f)
    parse_full_stats_v3(data)
