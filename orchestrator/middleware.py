"""Beta access control middleware for the solar orchestrator."""

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
