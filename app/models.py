from pydantic import BaseModel
from typing import List, Dict

class DatasetOut(BaseModel):
    dataset_id: str

class AnalysisOut(BaseModel):
    dataset_id: str
    record_count: int
    missing_values: int
    status: str