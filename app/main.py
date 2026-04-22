from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from app.api.router import router as api_router

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

app = FastAPI(
    title="Lawcidity API",
    docs_url="/api/docs",
    redoc_url=None,
    openapi_url="/api/openapi.json",
)

app.include_router(api_router)
