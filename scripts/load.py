import logging
from sqlalchemy import create_engine
import os

# ---------------- LOGGING ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- LOAD FUNCTION ---------------- #
def load_data(df, db_path="data/db/ecommerce.db", table_name="sales_data"):
    """
    Load transformed data into SQLite database
    """

    logging.info("🚀 Starting data load (SQLite)")

    try:
        if df is None or df.empty:
            logging.error("❌ No data to load")
            return

        # Create folder if not exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Create SQLite connection
        engine = create_engine(f"sqlite:///{db_path}")

        logging.info(f"🔌 Connected to SQLite DB: {db_path}")

        # Load data into table
        df.to_sql(table_name, engine, if_exists="replace", index=False)

        logging.info(f"💾 Data loaded into table: {table_name}")
        logging.info("✅ Load process completed")

    except Exception as e:
        logging.error(f"❌ Load error: {e}")