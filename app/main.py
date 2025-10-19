import io
import pandas as pd
from dateutil.parser import parse as parse_date
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from typing import Optional

from . import db
from .schemas import UploadResponse, SummaryResponse

app = FastAPI(title="Ecommerce Summary API", version="1.0")

REQUIRED_COLUMNS = ["transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"]


@app.post("/upload", response_model=UploadResponse)
async def upload_csv(file: UploadFile = File(...)):
    # Validate file type
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    # Read the uploaded file asynchronously
    contents = await file.read()
    buffer = io.BytesIO(contents)

    # Validate CSV headers
    try:
        df_head = pd.read_csv(buffer, nrows=0)
        missing = [c for c in REQUIRED_COLUMNS if c not in df_head.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="Empty CSV file")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read CSV header: {e}")

    # Reset buffer for full reading
    buffer.seek(0)

    # Initialize or reset database
    db.init_db(replace=True)

    chunksize = 100_000
    inserted_total = 0

    try:
        for chunk in pd.read_csv(buffer, chunksize=chunksize, parse_dates=["timestamp"]):
            chunk = chunk[REQUIRED_COLUMNS]
            chunk["timestamp"] = chunk["timestamp"].apply(lambda x: x.isoformat())
            chunk["transaction_amount"] = chunk["transaction_amount"].astype(float)
            rows = list(chunk.itertuples(index=False, name=None))
            inserted_total += db.insert_many(rows)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process CSV: {e}")

    return UploadResponse(status="ok", inserted_rows=inserted_total, message="File processed successfully")


@app.get("/summary/{user_id}", response_model=SummaryResponse)
def get_summary(user_id: int, start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    if user_id <= 0:
        raise HTTPException(status_code=400, detail="user_id must be positive integer")

    start_iso, end_iso = None, None
    try:
        if start:
            start_iso = parse_date(start).isoformat()
        if end:
            end_iso = parse_date(end).isoformat()
    except Exception:
        raise HTTPException(status_code=400, detail="start/end must be valid ISO datetime strings")

    count, min_v, max_v, avg_v = db.query_summary(user_id, start_iso, end_iso)
    return SummaryResponse(
        user_id=user_id,
        start=start_iso,
        end=end_iso,
        count=int(count or 0),
        min=float(min_v) if min_v is not None else None,
        max=float(max_v) if max_v is not None else None,
        mean=float(avg_v) if avg_v is not None else None,
    )
