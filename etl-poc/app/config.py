from pydantic import BaseModel
from dotenv import load_dotenv
import os
load_dotenv()
class Settings(BaseModel):
    app_env: str = os.getenv("APP_ENV", "local")
    api_key: str = os.getenv("API_KEY", "dev-local-key")
    database_url: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/etl")
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    backup_format: str = os.getenv("BACKUP_FORMAT", "parquet")
settings = Settings()
