from fastapi import APIRouter

from app.api.v1.endpoints import predict

router = APIRouter()
router.include_router(predict.router, prefix="/ml", tags=["ML"])
