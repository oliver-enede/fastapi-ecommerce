This project provides a small FastAPI service with two endpoints:

POST /upload — upload a CSV file of transactions; the server validates and imports the data into a local SQLite database.

GET /summary/{user_id} — returns max, min, and mean of transaction_amount for the specified user_id and optional date range (start, end).

The API handles large datasets (e.g., 1 million rows) using chunked CSV processing.

Setup requirements:
Python 3.9+

Installation:
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

Running the API:
uvicorn app.main:app --reload --port 8000
Then open http://127.0.0.1:8000/docs to use the interactive Swagger UI.

Running Tests:
pytest -q


POST /upload
Uploads a CSV file with the following headers:
transaction_id, user_id, product_id, timestamp, transaction_amount

Response:
{
  "status": "ok",
  "inserted_rows": 1000000,
  "message": "File processed"
}

GET /summary/{user_id}
Optional query parameters: start, end (ISO 8601 timestamps)

Response:
{
  "user_id": 1,
  "start": "2024-01-01T00:00:00",
  "end": "2024-12-31T23:59:59",
  "count": 250,
  "min": 12.3,
  "max": 495.5,
  "mean": 240.7
}


CSV data is stored in an SQLite database under data/transactions.db.
To regenerate the dummy dataset, run python generate_dummy.py.