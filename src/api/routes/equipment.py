"""Equipment lookup routes for CEC module and inverter databases."""

from functools import lru_cache

from fastapi import APIRouter

from src.pysam_integration.cec_database import CECDatabase

router = APIRouter(prefix="/analyses/equipment", tags=["equipment"])


@lru_cache(maxsize=1)
def _get_cec_db() -> CECDatabase:
    """Return a cached CECDatabase singleton."""
    return CECDatabase()


@router.get("/modules")
def list_modules(search: str | None = None) -> dict:
    """List valid CEC module names, optionally filtered by search term."""
    results = _get_cec_db().list_modules(search_term=search)
    return {"count": len(results), "modules": results}


@router.get("/inverters")
def list_inverters(search: str | None = None) -> dict:
    """List valid CEC inverter names, optionally filtered by search term."""
    results = _get_cec_db().list_inverters(search_term=search)
    return {"count": len(results), "inverters": results}
