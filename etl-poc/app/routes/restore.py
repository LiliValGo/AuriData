from fastapi import APIRouter, Depends, HTTPException, Header, Query
from pathlib import Path
import pandas as pd
from app.config import settings
from app.deps.db import get_conn, execute_many
router = APIRouter()
def require_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="invalid api key")
@router.post("/restore", dependencies=[Depends(require_api_key)])
def restore(table: str = Query(...), path: str = Query(...), mode: str = Query("truncate")):
    if table not in ("departments", "jobs", "hired_employees"):
        raise HTTPException(status_code=400, detail="unsupported table")
    p = Path(path)
    if not p.exists():
        raise HTTPException(status_code=404, detail="backup file not found")
    if p.suffix == ".parquet":
        df = pd.read_parquet(p)
    elif p.suffix == ".avro":
        from fastavro import reader
        rows = []
        with p.open("rb") as fo:
            for r in reader(fo):
                rows.append(r)
        df = pd.DataFrame(rows)
    else:
        raise HTTPException(status_code=400, detail="unsupported file type")
    cols = list(df.columns)
    placeholders = ", ".join([f"%({c})s" for c in cols])
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    with get_conn() as conn:
        with conn.cursor() as cur:
            if mode == "truncate":
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
        execute_many(conn, sql, df.to_dict(orient="records"))
        conn.commit()
    return {"restored": len(df)}
