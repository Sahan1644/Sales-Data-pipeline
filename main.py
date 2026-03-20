from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

def run_pipeline():
    # Extract
    df = extract_data("data/raw/events.csv", sample_size=100000)

    if df is not None:
        # Transform
        transformed_df = transform_data(df)

        if transformed_df is not None:
            # Load (SQLite)
            load_data(transformed_df)

if __name__ == "__main__":
    run_pipeline()