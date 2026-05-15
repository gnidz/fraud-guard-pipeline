import json
import time
from kafka import KafkaProducer
import os
import sys
import random

# Add project root to sys.path for cross-module imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.utils.telemetry import update_stats, save_raw_txn

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'financial-transactions'
USER_PROFILES_FILE = os.path.join(BASE_DIR, "data", "user_profiles.json")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_users():
    if os.path.exists(USER_PROFILES_FILE):
        with open(USER_PROFILES_FILE, 'r') as f:
            return json.load(f)
    print("Warning: user_profiles.json not found. Generating dummy users.")
    return [{"id": f"C{random.randint(100000, 999999)}", "is_vip": False, "avg_balance": 1000}]

def generate_transaction(step, users):
    types = ['PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN']
    is_fraud = random.random() < 0.10 # 10% underlying fraud chance
    
    # Pick a real user from our DB
    user = random.choice(users)
    
    if is_fraud:
        row = {
            "step": step,
            "type": "TRANSFER",
            "amount": round(random.uniform(5000.0, 10000.0), 2),
            "nameOrig": user["id"],
            "oldbalanceOrg": round(random.uniform(5000.0, 10000.0), 2),
            "newbalanceOrig": 0.0,
            "nameDest": f"M{random.randint(10000, 99999)}",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFraud": 1,
            "isFlaggedFraud": 0 # Logic moved to Spark/Agent
        }
    else:
        # Simulate normal behavior based on their profile
        if user["is_vip"]:
            amount = round(random.uniform(1000.0, 10000.0), 2)
            txn_type = random.choices(types, weights=[0.2, 0.4, 0.1, 0.1, 0.2])[0]
        else:
            amount = round(random.uniform(10.0, 800.0), 2)
            txn_type = random.choices(types, weights=[0.5, 0.1, 0.2, 0.1, 0.1])[0]
            
        row = {
            "step": step,
            "type": txn_type,
            "amount": amount,
            "nameOrig": user["id"],
            "oldbalanceOrg": user["avg_balance"],
            "newbalanceOrig": max(0, user["avg_balance"] - amount),
            "nameDest": f"M{random.randint(10000, 99999)}",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFraud": 0,
            "isFlaggedFraud": 0
        }
    return row

def stream_simulated_data(producer):
    print("[PRODUCER] Generating transactions (Raw Mode)...")
    users = get_users()
    step = 1
    
    while True:
        row = generate_transaction(step, users)
        producer.send(TOPIC_NAME, value=row)
        
        # We no longer send to 'fraud-alerts' here. 
        # Spark will decide what is an anomaly.
        
        update_stats("total")
        save_raw_txn(row)
        
        status_tag = 'REAL-FRAUD' if row['isFraud'] else 'NORMAL'
        print(f"[{status_tag}] Step {step}: ${row['amount']} {row['type']} from {row['nameOrig']}")
        step += 1
        time.sleep(random.uniform(1.0, 3.0))

if __name__ == "__main__":
    producer = create_producer()
    try:
        stream_simulated_data(producer)
    except KeyboardInterrupt:
        producer.close()
