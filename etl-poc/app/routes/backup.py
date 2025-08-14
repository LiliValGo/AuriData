from fastapi import APIRouter, Depends, HTTPException, Header, Query
from pathlib import Path
from datetime import datetime
from app.config import settings
from app.deps.db import get_conn, fetch_df
router = APIRouter()
def require_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="invalid api key")
@router.post("/backup", dependencies=[Depends(require_api_key)])
def backup(table: str = Query(...), format: str = Query("parquet")):
    if table not in ("departments", "jobs", "hired_employees"):
        raise HTTPException(status_code=400, detail="unsupported table")
    fmt = format.lower()
    if fmt not in ("parquet", "avro"):
        raise HTTPException(status_code=400, detail="format must be parquet or avro")
    with get_conn() as conn:
        df = fetch_df(conn, f"SELECT * FROM {table}")
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    outdir = Path(f"app/backups/{fmt}/{ts}")
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / f"{table}.{fmt}"
    if fmt == "parquet":
        df.to_parquet(out, index=False)
    else:
        from fastavro import parse_schema, writer
        import json
        schema_path = Path(f"app/schemas/{table}.avsc")
        if not schema_path.exists():
            raise HTTPException(status_code=500, detail=f"schema {schema_path} missing")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        parsed = parse_schema(schema)
        with out.open("wb") as fo:
            writer(fo, parsed, df.to_dict(orient="records"))
    return {"path": str(out)}
