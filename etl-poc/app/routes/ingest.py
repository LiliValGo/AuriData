from fastapi import APIRouter, Depends, HTTPException, Header
from typing import List, Dict
from app.config import settings
from app.ingest.loader import insert_batch
router = APIRouter()
def require_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="invalid api key")
@router.post("/ingest", dependencies=[Depends(require_api_key)])
def ingest(body: Dict):
    table = body.get("table")
    rows: List[Dict] = body.get("rows", [])
    if not table or not isinstance(rows, list) or len(rows) == 0:
        raise HTTPException(status_code=400, detail="table and rows[] are required")
    res = insert_batch(table, rows)
    return res
