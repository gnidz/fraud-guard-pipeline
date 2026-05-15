import json
import os
from datetime import datetime

# Define base directory relative to this file (src/utils/telemetry.py -> project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

REPORT_FILE = os.path.join(DATA_DIR, "investigations.json")
TXN_FILE = os.path.join(DATA_DIR, "recent_txns.json")

def save_investigation_report(account_id, amount, status, investigation_text):
    report = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "account_id": account_id,
        "amount": amount,
        "status": status,
        "summary": investigation_text
    }
    
    data = []
    if os.path.exists(REPORT_FILE):
        with open(REPORT_FILE, 'r') as f:
            try: data = json.load(f)
            except: data = []
            
    data.insert(0, report)
    with open(REPORT_FILE, 'w') as f:
        json.dump(data[:500], f, indent=2)

def update_stats(category="total"):
    # category can be "total", "blocked", or "cleared"
    file_path = os.path.join(DATA_DIR, f"stats_{category}.log")
    with open(file_path, 'a') as f:
        f.write('1')

def save_raw_txn(txn_data):
    data = []
    if os.path.exists(TXN_FILE):
        with open(TXN_FILE, 'r') as f:
            try: data = json.load(f)
            except: data = []
            
    # Add timestamp for UI
    txn_data['_time'] = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    data.insert(0, txn_data)
    
    with open(TXN_FILE, 'w') as f:
        json.dump(data[:500], f)
