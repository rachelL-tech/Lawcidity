from fastapi import FastAPI
from app.api_v1.router import router as v1_router

app = FastAPI(title="Lawcidity API")

# v1 API
app.include_router(v1_router)