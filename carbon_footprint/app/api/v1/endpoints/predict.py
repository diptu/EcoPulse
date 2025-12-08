import datetime
from typing import List

import torch
from fastapi import APIRouter
from pydantic import BaseModel, Field

# Import your model
from app.ml.ann import ANNModel  # Change to other models as needed

router = APIRouter()

# ---- Request Schema ----
class PredictRequest(BaseModel):
    user_id: str
    bill_id: str
    features: List[float] = Field(..., description="Normalized features for prediction")

# ---- Response Schema ----
class HourlyPrediction(BaseModel):
    hour: str
    kwh: float
    co2e_kg: float

class PredictResponse(BaseModel):
    user_id: str
    bill_id: str
    prediction_timestamp: str
    hourly_usage: List[HourlyPrediction]
    daily_summary: dict

# ---- Load Trained Model (example) ----
input_dim = 10  # Replace with actual feature size
model = ANNModel(input_dim=input_dim)
# model.load_state_dict(torch.load("app/ml/models/ann_model.pth"))  # Uncomment after training
model.eval()

# ---- Prediction Endpoint ----
@router.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    # Convert features to tensor
    x = torch.tensor(request.features, dtype=torch.float32).unsqueeze(0)  # shape: [1, input_dim]
    
    # ML prediction
    with torch.no_grad():
        predicted_kwh = model(x).item()
    
    # Generate hourly predictions (placeholder: same value for all hours)
    hourly_usage = []
    co2_per_kwh = 0.6  # Example, replace with dynamic grid intensity
    total_kwh = 0
    total_co2 = 0
    for h in range(24):
        kwh = round(predicted_kwh, 2)  # Replace with real hourly distribution
        co2e = round(kwh * co2_per_kwh, 2)
        hourly_usage.append({"hour": f"{h:02d}:00", "kwh": kwh, "co2e_kg": co2e})
        total_kwh += kwh
        total_co2 += co2e

    return PredictResponse(
        user_id=request.user_id,
        bill_id=request.bill_id,
        prediction_timestamp=datetime.datetime.utcnow().isoformat() + "Z",
        hourly_usage=hourly_usage,
        daily_summary={
            "total_kwh": round(total_kwh, 2),
            "total_co2e_kg": round(total_co2, 2)
        }
    )
    )
