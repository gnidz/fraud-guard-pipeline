import os
from datasets import load_dataset

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

def download_paysim():
    print("Downloading PaySim dataset from Kaggle...")
    try:
        import kaggle
        kaggle.api.authenticate()
        # Note: This might unzip multiple files, we usually want PS_20174392719_1491204439457_log.csv
        kaggle.api.dataset_download_files('ealaxi/paysim1', path=DATA_DIR, unzip=True)
        print(f"PaySim downloaded successfully to {DATA_DIR}.")
    except Exception as e:
        print(f"Error downloading from Kaggle: {e}")
        print("Please ensure your kaggle.json is correctly configured.")

def download_hf_dataset():
    print("Downloading Customer Support dataset from Hugging Face...")
    csv_path = os.path.join(DATA_DIR, "customer_support_data.csv")
    try:
        dataset = load_dataset("bitext/Bitext-customer-support-llm-chatbot-training-dataset", split="train")
        dataset.to_csv(csv_path)
        print(f"HF dataset downloaded and saved to {csv_path}.")
    except Exception as e:
        print(f"Error downloading from HF: {e}")

if __name__ == "__main__":
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    download_paysim()
    download_hf_dataset()
