import pandas as pd
import logging
import os

# ---------------- LOGGING SETUP ---------------- #
LOG_FILE = "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# ---------------- EXTRACT FUNCTION ---------------- #
def extract_data(file_path, output_path="data/raw/raw_copy.csv", sample_size=None):
    """
    Extract data from CSV and store raw copy in Data Lake

    Parameters:
    - file_path (str): Path to dataset
    - output_path (str): Path to save raw data
    - sample_size (int): Optional row limit for large datasets
    """

    logging.info("🚀 Starting data extraction")

    try:
        # Check if file exists
        if not os.path.exists(file_path):
            logging.error(f"❌ File not found: {file_path}")
            return None

        # Read dataset
        if sample_size:
            logging.warning(f"⚠️ Loading only {sample_size} rows (sampling enabled)")
            df = pd.read_csv(file_path, nrows=sample_size)
        else:
            df = pd.read_csv(file_path)

        logging.info("✅ Data loaded successfully")

        # Basic validation
        if df.empty:
            logging.error("❌ Dataset is empty")
            return None

        logging.info(f"📊 Rows: {df.shape[0]}, Columns: {df.shape[1]}")

        # Show column names
        logging.info(f"🧾 Columns: {list(df.columns)}")

        # Create directory if not exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save raw copy (Data Lake)
        df.to_csv(output_path, index=False)
        logging.info(f"💾 Raw data saved to Data Lake: {output_path}")

        logging.info("🎯 Extraction completed successfully")

        return df

    except pd.errors.ParserError as pe:
        logging.error(f"❌ Parsing error: {pe}")
    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}")

    return None


# ---------------- TEST RUN ---------------- #
if __name__ == "__main__":
    file_path = "data/raw/events.csv"

    # Use sample_size if dataset is too large
    df = extract_data(file_path, sample_size=100000)

    if df is not None:
        print("\n🔍 Preview:")
        print(df.head())