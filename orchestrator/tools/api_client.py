"""HTTP client for calling the solar modeling analysis API."""

import httpx


class AnalysisAPIClient:
    """Async HTTP client wrapping all 13 analysis API endpoints.

    Args:
        base_url: Base URL of the analysis API (e.g. "http://localhost:8000").
        api_key: Optional API key sent as X-API-Key header.
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: int = 120,
    ) -> None:
        headers: dict[str, str] = {}
        if api_key:
            headers["X-API-Key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=timeout,
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "AnalysisAPIClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # GET endpoints
    # ------------------------------------------------------------------

    async def health_check(self) -> dict:
        """Check API availability."""
        resp = await self._client.get("/health")
        resp.raise_for_status()
        return resp.json()

    async def search_modules(self, search: str = "") -> dict:
        """Search the CEC module database."""
        params = {"search": search} if search else {}
        resp = await self._client.get(
            "/analyses/equipment/modules", params=params
        )
        resp.raise_for_status()
        return resp.json()

    async def search_inverters(self, search: str = "") -> dict:
        """Search the CEC inverter database."""
        params = {"search": search} if search else {}
        resp = await self._client.get(
            "/analyses/equipment/inverters", params=params
        )
        resp.raise_for_status()
        return resp.json()

    async def list_load_types(self) -> dict:
        """List available DOE reference building types."""
        resp = await self._client.get("/analyses/load-types")
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # POST endpoints
    # ------------------------------------------------------------------

    async def build_rate(
        self, rate: dict, save_to_disk: bool = False
    ) -> dict:
        """Build and validate a rate schedule."""
        resp = await self._client.post(
            "/rates/build",
            json={"rate": rate, "save_to_disk": save_to_disk},
        )
        resp.raise_for_status()
        return resp.json()

    async def run_production(self, payload: dict) -> dict:
        """Run a production-only solar simulation."""
        resp = await self._client.post("/analyses/production", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def run_bill_savings(self, payload: dict) -> dict:
        """Run production + bill savings analysis."""
        resp = await self._client.post("/analyses/bill-savings", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def run_bess(self, payload: dict) -> dict:
        """Run BESS dispatch or sizing optimization."""
        resp = await self._client.post("/analyses/bess", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def run_buildability(self, payload: dict) -> dict:
        """Run buildable land assessment."""
        resp = await self._client.post("/analyses/buildability", json=payload)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Result retrieval endpoints
    # ------------------------------------------------------------------

    async def get_results(self, run_id: str) -> dict:
        """Retrieve results JSON for a completed run."""
        resp = await self._client.get(f"/analyses/{run_id}/results")
        resp.raise_for_status()
        return resp.json()

    async def get_report(self, run_id: str) -> bytes:
        """Download the PDF report for a completed run."""
        resp = await self._client.get(f"/analyses/{run_id}/report")
        resp.raise_for_status()
        return resp.content

    async def get_timeseries(self, run_id: str) -> bytes:
        """Download the 8760 timeseries CSV for a completed run."""
        resp = await self._client.get(f"/analyses/{run_id}/timeseries")
        resp.raise_for_status()
        return resp.content

    # ------------------------------------------------------------------
    # Tool dispatch
    # ------------------------------------------------------------------

    async def execute_tool(
        self, tool_name: str, arguments: dict
    ) -> dict | bytes:
        """Dispatch a tool call to the appropriate method.

        Args:
            tool_name: One of the 12 tool names from TOOL_DEFINITIONS.
            arguments: The arguments dict from OpenAI's function call.

        Returns:
            API response as dict (JSON endpoints) or bytes (file downloads).

        Raises:
            ValueError: If tool_name is not recognized.
        """
        if tool_name == "health_check":
            return await self.health_check()
        elif tool_name == "search_modules":
            return await self.search_modules(
                search=arguments.get("search", "")
            )
        elif tool_name == "search_inverters":
            return await self.search_inverters(
                search=arguments.get("search", "")
            )
        elif tool_name == "list_load_types":
            return await self.list_load_types()
        elif tool_name == "build_rate":
            return await self.build_rate(
                rate=arguments["rate"],
                save_to_disk=arguments.get("save_to_disk", False),
            )
        elif tool_name == "run_production":
            return await self.run_production(arguments)
        elif tool_name == "run_bill_savings":
            return await self.run_bill_savings(arguments)
        elif tool_name == "run_bess":
            return await self.run_bess(arguments)
        elif tool_name == "run_buildability":
            return await self.run_buildability(arguments)
        elif tool_name == "get_results":
            return await self.get_results(run_id=arguments["run_id"])
        elif tool_name == "get_report":
            return await self.get_report(run_id=arguments["run_id"])
        elif tool_name == "get_timeseries":
            return await self.get_timeseries(run_id=arguments["run_id"])
        else:
            raise ValueError(f"Unknown tool: {tool_name}")
