from app.ingest.validators import validate_record
import pytest
from datetime import datetime, timedelta
def test_hired_employee_future_datetime():
    future = (datetime.utcnow() + timedelta(days=1)).isoformat()
    with pytest.raises(Exception):
        validate_record("hired_employees", {"id": 1, "name": "A", "datetime": future, "department_id": 1, "job_id": 1})
