import os
import glob

# Define project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

def reset_system():
    print("[*] Starting System Reset...")
    
    # 1. Clear Log files
    log_files = glob.glob(os.path.join(DATA_DIR, "stats_*.log"))
    for f in log_files:
        try:
            os.remove(f)
            print(f"    - Deleted: {os.path.basename(f)}")
        except: pass

    # 2. Clear JSON reports
    json_files = ["investigations.json", "recent_txns.json"]
    for j in json_files:
        path = os.path.join(DATA_DIR, j)
        if os.path.exists(path):
            try:
                os.remove(path)
                print(f"    - Deleted: {j}")
            except: pass

    # 3. Create fresh empty logs (to avoid size-checking errors in backend)
    fresh_logs = ["stats_total.log", "stats_blocked.log", "stats_cleared.log"]
    for l in fresh_logs:
        with open(os.path.join(DATA_DIR, l), 'w') as f:
            pass # Just create empty file

    print("[SUCCESS] All live session data has been cleared.")
    print("[NOTE] Historical behavior database (historical_data.db) was NOT deleted.")

if __name__ == "__main__":
    reset_system()
