import random
import uuid
from pathlib import Path
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

# --- Configuration ---
NUM_ROWS = 1_000_000
OUTPUT_FILE = '../data/mock_transactions.parquet'
CATEGORIES = ['Groceries', 'Electronics', 'Clothing', 'Entertainment', 'Dining', 'Utilities', 'Travel', 'Health']
TOTAL_USERS = 50_000  # Creating a pool of 50k unique users to simulate repeat transactions
CHUNK_SIZE = 100_000

# Set up a date range for timestamps (e.g., transactions from the last 365 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
start_ts = int(start_date.timestamp())
end_ts = int(end_date.timestamp())

def generate_transactions(num_rows, filename):
    print(f"Starting generation of {num_rows:,} rows...")

    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema = pa.schema([
        ('transaction_id', pa.string()),
        ('user_id', pa.int64()),
        ('amount', pa.float64()),
        ('category', pa.string()),
        ('timestamp', pa.string()),
    ])

    writer = pq.ParquetWriter(str(output_path), schema)
    try:
        for start in range(0, num_rows, CHUNK_SIZE):
            rows_in_chunk = min(CHUNK_SIZE, num_rows - start)

            transaction_ids = [str(uuid.uuid4()) for _ in range(rows_in_chunk)]
            user_ids = [random.randint(1, TOTAL_USERS) for _ in range(rows_in_chunk)]
            amounts = [round(random.uniform(2.50, 1500.00), 2) for _ in range(rows_in_chunk)]
            categories = [random.choice(CATEGORIES) for _ in range(rows_in_chunk)]
            timestamps = [
                datetime.fromtimestamp(random.randint(start_ts, end_ts)).strftime('%Y-%m-%d %H:%M:%S')
                for _ in range(rows_in_chunk)
            ]

            table = pa.Table.from_arrays(
                [transaction_ids, user_ids, amounts, categories, timestamps],
                schema=schema,
            )
            writer.write_table(table)

            generated = start + rows_in_chunk
            print(f"Generated {generated:,} rows...")
    finally:
        writer.close()

    print(f"Success! Dataset saved locally as '{filename}'.")

if __name__ == "__main__":
    generate_transactions(NUM_ROWS, OUTPUT_FILE)
