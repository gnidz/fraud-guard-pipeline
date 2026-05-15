import json
import os
import sys
from kafka import KafkaConsumer
import concurrent.futures

# Add project root to sys.path for cross-module imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.agents.agent import run_agent

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'fraud-alerts'

def process_anomaly(anomaly_data):
    try:
        print(f"\n[!] ALERT RECEIVED: {json.dumps(anomaly_data, indent=2)}")
        
        # Extract key info for the agent
        account_id = anomaly_data.get('nameOrig', 'Unknown')
        amount = anomaly_data.get('amount', 0.0)
        txn_type = anomaly_data.get('type', 'UNKNOWN')
        
        event_description = f"User {account_id} attempted a {txn_type} of ${amount}. Flagged for review by the streaming engine. Please investigate."
        
        # Trigger the AI Agent.
        run_agent(event_description, account_id, amount)
    except Exception as e:
        print(f"Error processing anomaly: {e}")

def listen_for_anomalies():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='fraud-agent-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"[*] Agent Listener started. Monitoring topic '{TOPIC_NAME}' for anomalies with PARALLEL processing...")
    
    # Using ThreadPoolExecutor to run LLM inferences concurrently.
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
        try:
            for message in consumer:
                executor.submit(process_anomaly, message.value)
                
        except KeyboardInterrupt:
            print("\n[*] Listener stopped by user.")
        finally:
            consumer.close()

if __name__ == "__main__":
    listen_for_anomalies()
