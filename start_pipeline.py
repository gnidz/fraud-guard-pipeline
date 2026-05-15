import subprocess
import time
import sys
import os

# Define project root (now it's just the folder this file is in)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print("==================================================")
print(" Fraud Guard - Automated Pipeline Starter (DAG) ")
print("==================================================")
print("This script will start the pipeline components in the correct order.\n")

processes = []

def start_process(name, command):
    print(f"[*] Starting {name}...")
    # Start the process in the background
    proc = subprocess.Popen(command, shell=True)
    processes.append((name, proc))
    return proc

try:
    # 1. Spark Engine (Data Processor)
    spark_cmd = os.path.join(BASE_DIR, "infra", "run_spark_in_docker.bat")
    start_process("Spark Engine", spark_cmd)
    
    print("    -> Waiting 15 seconds for Spark to initialize and create Iceberg tables...")
    for i in range(15, 0, -1):
        sys.stdout.write(f"\r       {i}s remaining...")
        sys.stdout.flush()
        time.sleep(1)
    print("\n")

    # 2. Agent Listener (AI Investigator)
    agent_cmd = f"{sys.executable} {os.path.join(BASE_DIR, 'src', 'agents', 'agent_listener.py')}"
    start_process("Agent Listener", agent_cmd)
    
    print("    -> Waiting 5 seconds for AI Agent to connect to Kafka...")
    time.sleep(5)
    print("")

    # 3. Kafka Producer (Data Stream)
    producer_cmd = f"{sys.executable} {os.path.join(BASE_DIR, 'src', 'pipeline', 'producer.py')}"
    start_process("Kafka Producer", producer_cmd)

    print("\n==================================================")
    print("[SUCCESS] All pipeline components are running!")
    print(f"Go to the Web Dashboard ({os.path.join(BASE_DIR, 'web', 'index.html')}) to view the live data.")
    print("Press Ctrl+C here to stop all processes.")
    print("==================================================\n")
    
    # Keep the script running
    while True:
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n\n[!] KeyboardInterrupt detected. Stopping pipeline...")
    for name, proc in processes:
        print(f"[*] Stopping {name}...")
        try:
            # On Windows, terminating a shell process might not kill children.
            subprocess.run(f"taskkill /F /T /PID {proc.pid}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"    Failed to stop {name}: {e}")
            
    print("\nPipeline stopped successfully.")
    sys.exit(0)
