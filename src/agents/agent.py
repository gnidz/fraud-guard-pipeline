import os
import sys
import sqlite3
from langchain_ollama import ChatOllama
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from langchain_community.vectorstores import Chroma
from langchain_ollama import OllamaEmbeddings

# Add project root to sys.path for cross-module imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.utils.telemetry import save_investigation_report, update_stats

DB_FILE = os.path.join(BASE_DIR, "data", "historical_data.db")
CHROMA_DIR = os.path.join(BASE_DIR, "data", "chroma_db")

# Initialize local LLM
print(f"[*] Initializing ChatOllama with model 'llama3.2' at http://localhost:11434")
llm = ChatOllama(
    model="llama3.2:latest", 
    temperature=0,
    base_url="http://localhost:11434"
)

@tool
def search_policy(query: str) -> str:
    """Queries the ChromaDB to retrieve fraud resolution rules and policies."""
    print(f"[*] Tool: search_policy called with query: {query}")
    embeddings = OllamaEmbeddings(model="all-minilm:latest", base_url="http://localhost:11434")
    try:
        vectorstore = Chroma(persist_directory=CHROMA_DIR, embedding_function=embeddings, collection_name="policies")
        docs = vectorstore.similarity_search(query or "fraud rules", k=2)
        res = "\n\n".join([d.page_content for d in docs]) if docs else "No specific policy found."
        print(f"[*] Tool: search_policy returning {len(docs)} documents.")
        return res
    except Exception as e:
        print(f"[!] Tool: search_policy Error: {e}")
        return f"Error: {str(e)}"

def query_transactions(account_id: str) -> str:
    """Executes a real SQL query against the historical database to analyze a user's past behavior and patterns."""
    print(f"[*] Extracting behavioral data for account: {account_id}")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 1. Basic Stats
        cursor.execute('''
            SELECT COUNT(*), AVG(amount), MAX(amount) 
            FROM transactions WHERE nameOrig = ?
        ''', (account_id,))
        total_txns, avg_amount, max_amount = cursor.fetchone()
        
        if total_txns == 0 or total_txns is None:
            return f"No historical records found for account {account_id}. This is a NEW ACCOUNT (High Risk if amount is large)."

        avg_amount = avg_amount if avg_amount else 0.0
        max_amount = max_amount if max_amount else 0.0

        # 2. Velocity Check (Last 24 Hours - simulating based on the 'step' or timestamp)
        cursor.execute('''
            SELECT COUNT(*) FROM transactions 
            WHERE nameOrig = ? AND timestamp > datetime('now', '-3 days')
        ''', (account_id,))
        recent_count = cursor.fetchone()[0]

        # 3. Smurfing / Structuring Check (High frequency of small amounts OR amounts just below $10k reporting limit)
        cursor.execute('''
            SELECT COUNT(*) FROM transactions 
            WHERE nameOrig = ? AND ((amount < 500 AND type='TRANSFER') OR (amount >= 9000 AND amount < 10000)) AND timestamp > datetime('now', '-3 days')
        ''', (account_id,))
        smurfing_count = cursor.fetchone()[0]

        # 4. Pass-Through / Mule Account Check (Money comes in and goes right back out)
        # We look for a pattern where the total incoming roughly matches total outgoing recently.
        cursor.execute('''
            SELECT 
                SUM(CASE WHEN type IN ('CASH_IN') THEN amount ELSE 0 END) as total_in,
                SUM(CASE WHEN type IN ('TRANSFER', 'CASH_OUT') THEN amount ELSE 0 END) as total_out
            FROM transactions 
            WHERE nameOrig = ? AND timestamp > datetime('now', '-3 days')
        ''', (account_id,))
        flow_data = cursor.fetchone()
        total_in = flow_data[0] if flow_data[0] else 0
        total_out = flow_data[1] if flow_data[1] else 0
        is_pass_through = (total_in > 0) and (abs(total_in - total_out) / total_in < 0.1) # Within 10%

        # 5. Get last 5 transactions for context
        cursor.execute('''
            SELECT type, amount, timestamp FROM transactions 
            WHERE nameOrig = ? ORDER BY timestamp DESC LIMIT 5
        ''', (account_id,))
        recent = cursor.fetchall()
        conn.close()
        
        history_str = "\n".join([f"  * {r[2]}: {r[0]} ${r[1]:.2f}" for r in recent])
        profile_type = "VIP / HIGH-VOLUME" if avg_amount > 2000 else "STANDARD RETAIL"
        
        res = f"""
[BEHAVIORAL ANALYSIS FOR {account_id}]
- Profile: {profile_type}
- Lifetime Txns: {total_txns}
- Historical Avg: ${avg_amount:.2f} | Max: ${max_amount:.2f}
- Velocity: {recent_count}
- Smurfing Flags: {smurfing_count}
- Pass-Through/Mule Risk: {'HIGH' if is_pass_through else 'LOW'}
- Recent Activity:
{history_str}
"""
        return res
    except Exception as e:
        return f"Error querying database: {e}"

@tool
def block_account(account_id: str, reason: str) -> str:
    """Blocks the account and records the reason."""
    print(f"[*] Tool: block_account called for {account_id}. Reason: {reason}")
    return f"SUCCESS: Account {account_id} blocked. Reason: {reason}"

@tool
def notify_customer(account_id: str, message: str) -> str:
    """Sends a warning notification/SMS to the customer about suspicious activity without blocking."""
    print(f"[*] Tool: notify_customer called for {account_id}. Message: {message}")
    return f"SUCCESS: Notification sent to {account_id}: {message}"

tools = [search_policy, block_account, notify_customer]
agent_executor = create_react_agent(llm, tools)

def run_agent(anomaly_event: str, account_id: str = "Unknown", amount: float = 0.0):
    print(f"\n[!] Agent investigating: {anomaly_event}")
    
    # Inject data directly to avoid LLM hallucinating tool outputs
    behavioral_data = query_transactions(account_id)
    
    prompt = f"""You are a Lead AML & Fraud Strategist at a top-tier bank. A transaction was flagged: {anomaly_event}.

[USER BEHAVIORAL DATA]
{behavioral_data}

GOAL: Apply rigorous Anti-Money Laundering (AML) and Fraud prevention rules while protecting genuine customers.

DECISION FRAMEWORK (HIERARCHICAL PRIORITY - EVALUATE IN ORDER):
1. CRITICAL FRAUD (BLOCK): If 'Smurfing Flags' > 3 OR 'Pass-Through/Mule Risk' is HIGH.
   - Action: You MUST call the 'block_account' tool.
   - Output: Your final message MUST start with exactly [BLOCKED].

2. SUSPICIOUS ACTIVITY (NOTIFY): If the transaction amount is a large perfect round number (e.g., exactly 5000.00, 8000.00) AND the user profile is 'STANDARD RETAIL'.
   - Action: You MUST call the 'notify_customer' tool (DO NOT block).
   - Output: Your final message MUST start with exactly [NOTIFIED].

3. AMBIGUOUS POLICY (RESEARCH): If the situation is complex or you are unsure.
   - Action: Call the 'search_policy' tool to query bank guidelines before making a final decision.

4. NORMAL / SAFE (CLEAR): If conditions 1 and 2 do NOT apply.
   - Action: Approve the transaction. No tools needed. Do not assume fraud just because the amount is high (VIPs make large transfers).
   - Output: Your final message MUST start with exactly [CLEARED].

INSTRUCTIONS:
- Evaluate the rules strictly in the order listed above.
- Ensure you call the required tools before generating your final response.
- Your final output MUST begin with the status tag ([BLOCKED], [CLEARED], or [NOTIFIED]), followed by a 1-2 sentence technical reason based ONLY on the provided data numbers.
"""
    
    investigation_summary = ""
    try:
        print("[*] Invoking AI Agent...")
        result = agent_executor.invoke({"messages": [("user", prompt)]})
        investigation_summary = result["messages"][-1].content
        print("[*] AI Agent response received.")

        lower_summary = investigation_summary.lower()

        # Determine Status
        if "[blocked]" in lower_summary:
            status = "BLOCKED"
        elif "[notified]" in lower_summary:
            status = "NOTIFIED"
        elif "[cleared]" in lower_summary:
            status = "CLEARED"
        else:
            # Fallback text analysis
            if any(word in lower_summary for word in ["block", "blocked", "is anomalous"]):
                status = "BLOCKED"
            elif any(word in lower_summary for word in ["notify", "notified", "warning"]):
                status = "NOTIFIED"
            else:
                status = "CLEARED"

        print(f"[*] Final decision: {status}")
        save_investigation_report(account_id, amount, status, investigation_summary)
        
        # Update Stats (mapping NOTIFIED to cleared for simplicity or creating a new log)
        if status == "BLOCKED":
            update_stats("blocked")
        elif status == "NOTIFIED":
            update_stats("cleared") # For now, notified doesn't stop the txn
        else:
            update_stats("cleared")
            
        return investigation_summary
    except Exception as e:
        error_msg = f"[SYSTEM ERROR] AI Engine Offline: {str(e)}"
        print(f"Agent Error: {error_msg}")
        save_investigation_report(account_id, amount, "ERROR", error_msg)
        return error_msg

if __name__ == "__main__":
    run_agent("User C100150 made a $9000 transfer.", "C100150", 9000.0)
