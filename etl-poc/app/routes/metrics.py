from fastapi import APIRouter, Depends, HTTPException, Header, Query
from app.config import settings
from app.deps.db import get_conn, fetch_df
router = APIRouter()
def require_api_key(x_api_key: str = Header(...)):
    if x_api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="invalid api key")
@router.get("/metrics/hired-per-quarter", dependencies=[Depends(require_api_key)])
def hired_per_quarter(year: int = Query(..., ge=1900, le=9999)):
    sql = """    SELECT d.name AS department, j.name AS job,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 1 THEN 1 ELSE 0 END) AS q1,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 2 THEN 1 ELSE 0 END) AS q2,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 3 THEN 1 ELSE 0 END) AS q3,
           SUM(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 4 THEN 1 ELSE 0 END) AS q4
    FROM hired_employees he
    JOIN departments d ON he.department_id = d.id
    JOIN jobs j ON he.job_id = j.id
    WHERE EXTRACT(YEAR FROM he.datetime) = %s
    GROUP BY d.name, j.name
    ORDER BY d.name, j.name;
    """
    with get_conn() as conn:
        df = fetch_df(conn, sql, (year,))
    return {"year": year, "rows": df.to_dict(orient="records")}
@router.get("/metrics/top-departments", dependencies=[Depends(require_api_key)])
def top_departments(year: int = Query(..., ge=1900, le=9999)):
    sql = """    WITH hired_counts AS (
      SELECT d.id, d.name AS department, COUNT(*) AS hired
      FROM hired_employees he
      JOIN departments d ON he.department_id = d.id
      WHERE EXTRACT(YEAR FROM he.datetime) = %s
      GROUP BY d.id, d.name
    ), avg_val AS (
      SELECT AVG(hired) AS avg_h FROM hired_counts
    )
    SELECT id, department, hired
    FROM hired_counts, avg_val
    WHERE hired > avg_h
    ORDER BY hired DESC;
    """
    with get_conn() as conn:
        df = fetch_df(conn, sql, (year,))
    return {"year": year, "rows": df.to_dict(orient="records")}
