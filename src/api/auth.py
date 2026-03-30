"""API key authentication middleware for the Solar Model API."""

import os

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response


class APIKeyMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces static API key authentication.

    Reads the expected key from the API_KEY environment variable.
    If API_KEY is not set, all requests pass through (auth disabled).
    If API_KEY is set, all requests except GET /health must include
    a matching X-API-Key header.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Check X-API-Key header against API_KEY env var."""
        api_key = os.environ.get("API_KEY")

        # Auth disabled when env var is unset
        if not api_key:
            return await call_next(request)

        # Health check is always exempt
        if request.url.path == "/health" and request.method == "GET":
            return await call_next(request)

        provided_key = request.headers.get("X-API-Key")

        if not provided_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing API key. Provide X-API-Key header."},
            )

        if provided_key != api_key:
            return JSONResponse(
                status_code=403,
                content={"detail": "Invalid API key."},
            )

        return await call_next(request)
