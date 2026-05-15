import pandas as pd
import os
import sys

# Add project root to sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_FILE = os.path.join(DATA_DIR, "customer_support_data.csv")
CHROMA_DIR = os.path.join(DATA_DIR, "chroma_db")

def ingest_data():
    if not os.path.exists(CSV_FILE):
        print(f"Dataset not found at {CSV_FILE}. Please run download_datasets.py first.")
        return

    print("Loading customer support data...")
    df = pd.read_csv(CSV_FILE).head(500)
    
    from langchain_community.document_loaders import DataFrameLoader
    if 'instruction' in df.columns and 'response' in df.columns:
        df['combined_text'] = "Policy: " + df['instruction'].astype(str) + " \nResolution: " + df['response'].astype(str)
        loader = DataFrameLoader(df, page_content_column="combined_text")
    else:
        text_col = df.columns[0]
        loader = DataFrameLoader(df, page_content_column=text_col)

    docs = loader.load()

    print("Splitting documents...")
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    splits = text_splitter.split_documents(docs)
    
    print("Initializing embeddings model...")
    from langchain_ollama import OllamaEmbeddings
    embeddings = OllamaEmbeddings(
        model="all-minilm:latest",
        base_url="http://localhost:11434"
    )

    print("Storing in ChromaDB...")
    from langchain_community.vectorstores import Chroma
    vectorstore = Chroma.from_documents(
        documents=splits,
        embedding=embeddings,
        persist_directory=CHROMA_DIR,
        collection_name="policies"
    )
    
    print(f"Ingestion complete. Embeddings stored in {CHROMA_DIR}")

if __name__ == "__main__":
    ingest_data()
