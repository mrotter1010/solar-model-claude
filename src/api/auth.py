"""Authentication middleware for the Solar Model API."""

import os

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response


class InviteCodeMiddleware(BaseHTTPMiddleware):
    """Middleware that enforces beta invite code access control.

    Reads allowed codes from the INVITE_CODES environment variable
    (comma-separated). If INVITE_CODES is not set or empty, all
    requests pass through (gate disabled for local dev).
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Check X-Invite-Code header against INVITE_CODES env var."""
        invite_codes_raw = os.environ.get("INVITE_CODES", "")
        invite_codes = [
            code.strip() for code in invite_codes_raw.split(",") if code.strip()
        ]

        # Gate disabled when env var is unset or empty
        if not invite_codes:
            return await call_next(request)

        # Health check is always exempt
        if request.url.path == "/health" and request.method == "GET":
            return await call_next(request)

        provided_code = request.headers.get("X-Invite-Code")

        if not provided_code or provided_code not in invite_codes:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing invite code"},
            )

        return await call_next(request)


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
