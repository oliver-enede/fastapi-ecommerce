from pydantic import BaseModel
from typing import Optional


class UploadResponse(BaseModel):
    status: str
    inserted_rows: int
    message: Optional[str] = None


class SummaryResponse(BaseModel):
    user_id: int
    start: Optional[str]
    end: Optional[str]
    count: int
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]