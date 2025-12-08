from fastapi import FastAPI

from app.api.v1.router import router as api_router

app = FastAPI(
    title="EcoPulse ML API",
    version="0.1.0",
    description="Predict hourly energy usage and CO2 emissions",
)

app.include_router(api_router)


@app.get("/")
def root():
    return {"message": "EcoPulse ML API is running"}
