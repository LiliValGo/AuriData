from fastapi import FastAPI
from app.routes.ingest import router as ingest_router
from app.routes.backup import router as backup_router
from app.routes.restore import router as restore_router
from app.routes.metrics import router as metrics_router
app = FastAPI(title="ETL PoC API", version="0.1.0")
app.include_router(ingest_router)
app.include_router(backup_router)
app.include_router(restore_router)
app.include_router(metrics_router)
@app.get("/health")
def health():
    return {"status": "ok"}
