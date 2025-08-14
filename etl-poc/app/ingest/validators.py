from pydantic import BaseModel, Field, field_validator
from datetime import datetime
class Department(BaseModel):
    id: int
    name: str = Field(min_length=2)
class Job(BaseModel):
    id: int
    name: str = Field(min_length=2)
class HiredEmployee(BaseModel):
    id: int
    name: str = Field(min_length=2)
    datetime: datetime
    department_id: int
    job_id: int
    @field_validator("datetime")
    @classmethod
    def not_future(cls, v: datetime):
        if v > datetime.utcnow():
            raise ValueError("datetime cannot be in the future")
        return v
SCHEMAS = {"departments": Department, "jobs": Job, "hired_employees": HiredEmployee}
def validate_record(table: str, data: dict):
    model = SCHEMAS.get(table)
    if not model:
        raise ValueError(f"Unsupported table: {table}")
    return model(**data).model_dump()
