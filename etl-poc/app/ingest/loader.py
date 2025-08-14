from typing import List, Dict
from app.deps.db import get_conn, execute_many
from app.ingest.validators import validate_record
INSERT_SQL = {
    "departments": "INSERT INTO departments (id, name) VALUES (%(id)s, %(name)s) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name",
    "jobs": "INSERT INTO jobs (id, name) VALUES (%(id)s, %(name)s) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name",
    "hired_employees": "INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (%(id)s, %(name)s, %(datetime)s, %(department_id)s, %(job_id)s) ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, datetime=EXCLUDED.datetime, department_id=EXCLUDED.department_id, job_id=EXCLUDED.job_id",
}
def insert_batch(table: str, rows: List[Dict]):
    valid_rows = []
    invalid = 0
    for r in rows:
        try:
            valid_rows.append(validate_record(table, r))
        except Exception as e:
            invalid += 1
            with open("app/logs/ingest_invalid.jsonl", "a", encoding="utf-8") as f:
                f.write(str({"table": table, "error": str(e), "row": r}) + "\n")
    if not valid_rows:
        return {"inserted": 0, "invalid": invalid}
    sql = INSERT_SQL[table]
    with get_conn() as conn:
        execute_many(conn, sql, valid_rows)
        conn.commit()
    with open("app/logs/ingest_valid.jsonl", "a", encoding="utf-8") as f:
        f.write(str({"table": table, "inserted": len(valid_rows)}) + "\n")
    return {"inserted": len(valid_rows), "invalid": invalid}
