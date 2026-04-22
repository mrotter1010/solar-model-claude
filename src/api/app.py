"""FastAPI application factory for the Solar Model API."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.auth import APIKeyMiddleware
from src.api.routes.analyses import router as analyses_router
from src.api.routes.batch import router as batch_router
from src.api.routes.equipment import router as equipment_router
from src.api.routes.lmp import router as lmp_router
from src.api.routes.optimization import router as optimization_router
from src.api.routes.rates import router as rates_router
from src.api.routes.results import router as results_router
from src.api.routes.uploads import router as uploads_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI instance with all routers included.
    """
    app = FastAPI(title="Solar Model API", version="1.0.0")

    cors_origins = [
        origin.strip()
        for origin in os.environ.get(
            "CORS_ORIGINS", "http://localhost:5173"
        ).split(",")
        if origin.strip()
    ]

    app.add_middleware(APIKeyMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health() -> dict:
        """Health check endpoint."""
        return {"status": "ok", "version": "1.0.0"}

    app.include_router(analyses_router)
    app.include_router(batch_router)
    app.include_router(equipment_router)
    app.include_router(lmp_router)
    app.include_router(optimization_router)
    app.include_router(rates_router)
    app.include_router(results_router)
    app.include_router(uploads_router)
    return app
