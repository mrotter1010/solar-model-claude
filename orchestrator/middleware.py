"""Beta access control and user identity middleware for the solar orchestrator."""

import logging
import os
import re

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

logger = logging.getLogger(__name__)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


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


class UserIdentityMiddleware(BaseHTTPMiddleware):
    """Middleware that reads and validates the X-User-Id header.

    Attaches the validated user ID to ``request.state.user_id`` so
    downstream endpoints can access it. The header is optional — missing
    headers result in a warning log and ``request.state.user_id = None``.
    Malformed (non-UUID) values are rejected with 400.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Validate X-User-Id header and attach to request state."""
        user_id = request.headers.get("X-User-Id")

        if not user_id:
            request.state.user_id = None
            if request.url.path != "/health":
                logger.warning(
                    "Missing X-User-Id header on %s %s",
                    request.method,
                    request.url.path,
                )
            return await call_next(request)

        if not _UUID_RE.match(user_id):
            return JSONResponse(
                status_code=400,
                content={"detail": "X-User-Id must be a valid UUID"},
            )

        request.state.user_id = user_id
        return await call_next(request)
