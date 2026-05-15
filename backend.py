from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import subprocess
import os
import sys
import asyncio
import json
import psutil
from kafka import KafkaProducer
from src.utils.telemetry import save_raw_txn, update_stats

# Define project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

DATA_DIR = os.path.join(BASE_DIR, "data")
WEB_DIR = os.path.join(BASE_DIR, "web")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global Kafka Producer for manual logic (if any left)
_producer = None
def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                request_timeout_ms=5000,
                connection_timeout_ms=5000
            )
        except Exception as e:
            print(f"Kafka Producer Error: {e}")
    return _producer

# Services paths
SERVICES = {
    "Infrastructure": f"docker-compose -f {os.path.join(BASE_DIR, 'infra', 'docker-compose.yml')} up -d",
    "RAG Ingestion": f"venv\\Scripts\\python.exe {os.path.join(BASE_DIR, 'src', 'agents', 'rag_ingestion.py')}",
    "Kafka Producer": f"venv\\Scripts\\python.exe {os.path.join(BASE_DIR, 'src', 'pipeline', 'producer.py')}",
    "Agent Listener": f"venv\\Scripts\\python.exe {os.path.join(BASE_DIR, 'src', 'agents', 'agent_listener.py')}",
    "Spark Engine": f"{os.path.join(BASE_DIR, 'infra', 'run_spark_in_docker.bat')}"
}

@app.get("/")
def get_index():
    return FileResponse(os.path.join(WEB_DIR, "index.html"))

@app.get("/status")
def get_status():
    status = {}
    for name in SERVICES:
        is_running = False
        if name == "Infrastructure":
            try:
                out = subprocess.check_output("docker ps --format \"{{.Names}}\"", shell=True).decode()
                is_running = "minio" in out and "kafka" in out
            except: is_running = False
        elif name == "Spark Engine":
            try:
                out = subprocess.check_output("docker top spark-iceberg", shell=True).decode()
                is_running = "spark-submit" in out or "java" in out
            except: is_running = False
        else:
            # Check for script execution in psutil
            script_path = SERVICES[name].split()[-1]
            script_name = os.path.basename(script_path)
            for p in psutil.process_iter(['cmdline']):
                try:
                    if p.info['cmdline'] and any(script_name in arg for arg in p.info['cmdline']):
                        is_running = True
                        break
                except: pass
        status[name] = "Running" if is_running else "Stopped"
    return status

@app.get("/telemetry")
def get_telemetry():
    def get_log_size(name):
        path = os.path.join(DATA_DIR, name)
        return os.path.getsize(path) if os.path.exists(path) else 0

    stats = {
        "total_txns": get_log_size("stats_total.log"), 
        "fraud_blocked": get_log_size("stats_blocked.log"),
        "fraud_cleared": get_log_size("stats_cleared.log")
    }
    
    investigations = []
    inv_path = os.path.join(DATA_DIR, "investigations.json")
    if os.path.exists(inv_path):
        with open(inv_path, "r") as f:
            try: investigations = json.load(f)
            except: pass

    raw_txns = []
    txn_path = os.path.join(DATA_DIR, "recent_txns.json")
    if os.path.exists(txn_path):
        with open(txn_path, "r") as f:
            try: raw_txns = json.load(f)
            except: pass
            
    return {
        "stats": stats, 
        "investigations": investigations,
        "raw_txns": raw_txns
    }

@app.post("/control/{action}/{service}")
def control_service(action: str, service: str):
    if service not in SERVICES: return {"status": "error"}
    
    if action == "start":
        subprocess.Popen(SERVICES[service], shell=True)
        return {"status": "starting"}
    else:
        script_path = SERVICES[service].split()[-1]
        script_name = os.path.basename(script_path)
        for p in psutil.process_iter(['cmdline']):
            try:
                if p.info['cmdline'] and any(script_name in arg for arg in p.info['cmdline']):
                    p.terminate()
            except: pass
        return {"status": "stopped"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await asyncio.sleep(1)
            await websocket.send_text("[System] Monitoring pulse...")
    except WebSocketDisconnect:
        print("Client disconnected")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
