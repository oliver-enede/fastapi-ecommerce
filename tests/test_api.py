import io
import csv
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

SAMPLE_ROWS = [
    ("tx1", 1, 10, "2024-09-01T12:00:00", 100.0),
    ("tx2", 1, 11, "2024-09-02T12:00:00", 50.0),
    ("tx3", 2, 12, "2024-09-01T13:00:00", 200.0),
]


def make_csv_bytes(rows):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"])
    for r in rows:
        writer.writerow(r)
    return buf.getvalue().encode("utf-8")


def test_upload_and_summary():
    csv_bytes = make_csv_bytes(SAMPLE_ROWS)
    files = {"file": ("sample.csv", csv_bytes, "text/csv")}

    # Upload CSV
    response = client.post("/upload", files=files)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["inserted_rows"] == 3

    # Summary for user 1 (overall)
    response = client.get("/summary/1")
    assert response.status_code == 200
    summary = response.json()
    assert summary["count"] == 2
    assert summary["min"] == 50.0
    assert summary["max"] == 100.0
    assert abs(summary["mean"] - 75.0) < 1e-6

    # Summary with date filter
    response = client.get("/summary/1?start=2024-09-02T00:00:00&end=2024-09-03T00:00:00")
    assert response.status_code == 200
    summary_range = response.json()
    assert summary_range["count"] == 1
    assert summary_range["min"] == 50.0


def test_invalid_file_upload():
    files = {"file": ("bad.txt", b"not a csv", "text/plain")}
    r = client.post("/upload", files=files)
    assert r.status_code == 400


def test_invalid_user_id():
    r = client.get("/summary/0")
    assert r.status_code == 400
    assert "user_id" in r.text
