import sqlite3
import random
from datetime import datetime, timedelta
import json
import os

# Define paths relative to the data folder
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(DATA_DIR, "historical_data.db")
USER_PROFILES_FILE = os.path.join(DATA_DIR, "user_profiles.json")

def init_db():
    print("[*] Initializing Historical Database...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create transactions table matching PaySim schema
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        step INTEGER,
        type TEXT,
        amount REAL,
        nameOrig TEXT,
        oldbalanceOrg REAL,
        newbalanceOrig REAL,
        nameDest TEXT,
        oldbalanceDest REAL,
        newbalanceDest REAL,
        isFraud INTEGER,
        isFlaggedFraud INTEGER,
        timestamp TEXT
    )
    ''')
    
    # Create indexes for fast querying
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_nameOrig ON transactions(nameOrig)')
    
    # Clear existing data
    cursor.execute('DELETE FROM transactions')
    
    users = []
    
    print("[*] Generating 1,000 unique users (800 Standard, 200 VIP)...")
    for i in range(1, 1001):
        is_vip = i <= 200 # First 200 are VIPs
        users.append({
            "id": f"C{100000 + i}",
            "is_vip": is_vip,
            "avg_balance": random.uniform(50000, 200000) if is_vip else random.uniform(500, 5000)
        })
        
    print("[*] Generating 15,000 historical transactions...")
    transactions = []
    types = ['PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN']
    
    base_time = datetime.now() - timedelta(days=365) # 1 year of history
    
    for step in range(1, 15001):
        user = random.choice(users)
        
        # Simulate amounts based on profile
        if user["is_vip"]:
            amount = round(random.uniform(1000.0, 15000.0), 2)
            txn_type = random.choices(types, weights=[0.2, 0.4, 0.1, 0.1, 0.2])[0] # VIPs transfer more
        else:
            amount = round(random.uniform(10.0, 800.0), 2)
            txn_type = random.choices(types, weights=[0.5, 0.1, 0.2, 0.1, 0.1])[0] # Normal users pay more
            
        old_balance = user["avg_balance"] + random.uniform(-1000, 1000)
        if old_balance < 0: old_balance = 0
        new_balance = old_balance - amount if txn_type in ['PAYMENT', 'TRANSFER', 'CASH_OUT'] else old_balance + amount
        if new_balance < 0: new_balance = 0
        
        # Increment time by approx 35 minutes per step to span a year
        txn_time = base_time + timedelta(minutes=step * 35)
        
        transactions.append((
            step,
            txn_type,
            amount,
            user["id"],
            round(old_balance, 2),
            round(new_balance, 2),
            f"M{random.randint(100000, 999999)}",
            0.0,
            0.0,
            0, # Historical data is mostly clean
            0,
            txn_time.strftime("%Y-%m-%d %H:%M:%S")
        ))
        
        if step % 5000 == 0:
            cursor.executemany('INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', transactions)
            transactions = []
            print(f"    -> Inserted {step}/15000 records...")

    if transactions:
        cursor.executemany('INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', transactions)

    conn.commit()
    conn.close()
    
    # Save users for the producer to use
    with open(USER_PROFILES_FILE, 'w') as f:
        json.dump(users, f)
        
    print(f"[SUCCESS] Database initialization complete! Saved to {DATA_DIR}")

if __name__ == "__main__":
    init_db()
