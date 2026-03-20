import csv
import random
import uuid
from datetime import datetime, timedelta

# --- Configuration ---
NUM_ROWS = 1_000_000
OUTPUT_FILE = '../data/mock_transactions.csv'
CATEGORIES = ['Groceries', 'Electronics', 'Clothing', 'Entertainment', 'Dining', 'Utilities', 'Travel', 'Health']
TOTAL_USERS = 50_000  # Creating a pool of 50k unique users to simulate repeat transactions

# Set up a date range for timestamps (e.g., transactions from the last 365 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=365)
start_ts = int(start_date.timestamp())
end_ts = int(end_date.timestamp())

def generate_transactions(num_rows, filename):
    print(f"Starting generation of {num_rows:,} rows...")
    
    # Open the file in write mode
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # 1. Write the header
        writer.writerow(['transaction_id', 'user_id', 'amount', 'category', 'timestamp'])
        
        # 2. Generate and write data row-by-row
        for i in range(num_rows):
            transaction_id = str(uuid.uuid4())
            user_id = random.randint(1, TOTAL_USERS)
            amount = round(random.uniform(2.50, 1500.00), 2)
            category = random.choice(CATEGORIES)
            
            # Generate a random timestamp within our defined range
            random_ts = random.randint(start_ts, end_ts)
            timestamp = datetime.fromtimestamp(random_ts).strftime('%Y-%m-%d %H:%M:%S')
            
            # Write the row to the CSV
            writer.writerow([transaction_id, user_id, amount, category, timestamp])
            
            # Print progress every 100,000 rows
            if (i + 1) % 100_000 == 0:
                print(f"Generated {i + 1:,} rows...")

    print(f"Success! Dataset saved locally as '{filename}'.")

if __name__ == "__main__":
    generate_transactions(NUM_ROWS, OUTPUT_FILE)
