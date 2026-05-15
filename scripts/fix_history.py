import json
import os

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_FILE = os.path.join(BASE_DIR, "data", "investigations.json")

if os.path.exists(REPORT_FILE):
    with open(REPORT_FILE, 'r') as f:
        data = json.load(f)
    
    fixed_count = 0
    for entry in data:
        summary = entry.get("summary", "").lower()
        if "[cleared]" in summary and entry["status"] == "BLOCKED":
            entry["status"] = "CLEARED"
            fixed_count += 1
        elif "should be cleared" in summary and entry["status"] == "BLOCKED":
             entry["status"] = "CLEARED"
             fixed_count += 1
             
    with open(REPORT_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Fixed {fixed_count} entries in {REPORT_FILE}")
else:
    print(f"File not found: {REPORT_FILE}")
