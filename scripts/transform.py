import pandas as pd
import logging
import os

# ---------------- LOGGING ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- TRANSFORM FUNCTION ---------------- #
def transform_data(df, output_path="data/processed/clean_data.csv"):
    """
    Clean and transform raw data

    Steps:
    - Handle missing values
    - Remove duplicates
    - Fix data types
    - Filter relevant data
    - Aggregate for insights
    """

    logging.info("🔄 Starting data transformation")

    try:
        # ---------------- VALIDATION ---------------- #
        if df is None or df.empty:
            logging.error("❌ Input dataframe is empty")
            return None

        # ---------------- HANDLE MISSING VALUES ---------------- #
        logging.info("🧹 Handling missing values")
        df = df.dropna()

        # ---------------- REMOVE DUPLICATES ---------------- #
        logging.info("🧹 Removing duplicates")
        df = df.drop_duplicates()

        # ---------------- DATA TYPE CONVERSION ---------------- #
        logging.info("🔧 Converting data types")
        df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')

        # Drop rows where conversion failed
        df = df.dropna(subset=['event_time'])

        # ---------------- FILTER ONLY PURCHASE EVENTS ---------------- #
        logging.info("🛒 Filtering purchase events")
        df = df[df['event_type'] == 'purchase']

        # ---------------- AGGREGATION ---------------- #
        logging.info("📊 Aggregating sales data")

        sales_summary = (
            df.groupby(['product_id'])
            .agg(purchase_count=('event_type', 'count'))
            .reset_index()
        )

        # ---------------- CREATE OUTPUT FOLDER ---------------- #
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # ---------------- SAVE PROCESSED DATA ---------------- #
        sales_summary.to_csv(output_path, index=False)

        logging.info(f"💾 Processed data saved to: {output_path}")
        logging.info("✅ Transformation completed")

        return sales_summary

    except Exception as e:
        logging.error(f"❌ Transformation error: {e}")
        return None


# ---------------- TEST RUN ---------------- #
if __name__ == "__main__":
    from extract import extract_data

    df = extract_data("data/raw/events.csv", sample_size=100000)

    if df is not None:
        result = transform_data(df)

        print("\n🔍 Transformed Data Preview:")
        print(result.head())