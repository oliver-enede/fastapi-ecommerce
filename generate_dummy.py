import csv
import random
from pathlib import Path
from faker import Faker

TRANSACTIONS = 1_000_000
HEADERS = ["transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"]

fake = Faker()

output_path = Path("dummy_transactions.csv")

with output_path.open(mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=HEADERS)
    writer.writeheader()

    for _ in range(TRANSACTIONS):
        writer.writerow({
            "transaction_id": fake.uuid4(),
            "user_id": fake.random_int(min=1, max=1000),
            "product_id": fake.random_int(min=1, max=500),
            "timestamp": fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
            "transaction_amount": round(random.uniform(5.0, 500.0), 2),
        })
