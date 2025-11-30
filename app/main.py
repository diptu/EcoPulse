"""User service runner with async table creation and FastAPI initialization."""

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from app.api.v1.routes import facility_router
from app.core.config import settings
from app.db.session import engine
from app.models.base import Base

# from user_service.app.api.v1.routes import (


# -------------------------
# Initialize FastAPI app
# -------------------------
app = FastAPI(
    title="Analytics Service",
    version="0.0.1",
    description="Service for ingesting and transforming energy datasets, running ML models,\
    and exposing APIs for carbon-intensity, CO₂e calculation, and energy-usage inference.\
    Includes ETL pipelines, scheduled Airflow jobs, and model-driven analytics.",
)

from fastapi import status

SERVER_HEALTH_DOCS = {
    "summary": "Server health",
    "description": "Check API server liveness.",
    "responses": {
        status.HTTP_200_OK: {"description": "Server is healthy"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Server health check failed"
        },
    },
}


# -----------------------------
# Root Endpoint
# -----------------------------
@app.get("/", **SERVER_HEALTH_DOCS)
async def root():
    async def server_check() -> bool:
        """
        Lightweight internal check.
        Replace/extend this with:
        - CPU/memory threshold checks
        - Internal service checks
        - Dependency readiness (cache, message broker, etc.)
        """
        return True  # Always true unless extended

    is_alive = await server_check()

    return {
        "status": status.HTTP_200_OK
        if is_alive
        else status.HTTP_500_INTERNAL_SERVER_ERROR,
        "server": "Server is healthy" if is_alive else "Server health check failed",
        "service": settings.SERVICE_NAME,
        "version": settings.SERVICE_VERSION,
    }


# -------------------------
# Include API routers
# -------------------------

app.include_router(facility_router)  ## place holder


# -------------------------
# Create tables on startup
# -------------------------
@app.on_event("startup")
async def create_tables():
    """Create all database tables asynchronously on app startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# -------------------------
# Custom OpenAPI schema with BearerAuth
# -------------------------
def custom_openapi():
    """Add BearerAuth security scheme to all endpoints except login & verify."""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Define Bearer token scheme
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
    }

    # Apply BearerAuth to all endpoints except login & verify
    for path, methods in openapi_schema["paths"].items():
        if path.startswith("/auth") or path.startswith("/users/verify"):
            continue
        for method in methods.values():
            method.setdefault("security", [{"BearerAuth": []}])

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
