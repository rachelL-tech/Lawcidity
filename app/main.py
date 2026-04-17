from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from app.api_v1.router import router as v1_router

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

app = FastAPI(title="Lawcidity API")

# v1 API
app.include_router(v1_router)
