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
def list_modules(
    search: str | None = None,
    min_stc: float | None = None,
    max_stc: float | None = None,
) -> dict:
    """List valid CEC module names, optionally filtered by search and STC range."""
    results = _get_cec_db().list_modules(
        search_term=search, min_stc=min_stc, max_stc=max_stc
    )
    return {"count": len(results), "modules": results}


@router.get("/inverters")
def list_inverters(
    search: str | None = None,
    min_paco: float | None = None,
    max_paco: float | None = None,
) -> dict:
    """List valid CEC inverter names, optionally filtered by search and Paco range."""
    results = _get_cec_db().list_inverters(
        search_term=search, min_paco=min_paco, max_paco=max_paco
    )
    return {"count": len(results), "inverters": results}
