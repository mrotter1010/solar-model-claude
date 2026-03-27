"""FastAPI application factory for the Solar Model API."""

from fastapi import FastAPI

from src.api.routes.analyses import router as analyses_router
from src.api.routes.equipment import router as equipment_router
from src.api.routes.results import router as results_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI instance with all routers included.
    """
    app = FastAPI(title="Solar Model API", version="1.0.0")

    @app.get("/health")
    def health() -> dict:
        """Health check endpoint."""
        return {"status": "ok", "version": "1.0.0"}

    app.include_router(analyses_router)
    app.include_router(equipment_router)
    app.include_router(results_router)
    return app
