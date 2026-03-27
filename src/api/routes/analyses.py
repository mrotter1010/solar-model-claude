"""Analysis routes for the Solar Model API."""

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException

from src.api.adapter import (
    bess_request_to_site_config,
    bill_savings_request_to_site_config,
    buildability_request_to_site_config,
    production_request_to_site_config,
)
from src.api.runner import (
    extract_bess_response,
    extract_bill_savings_response,
    extract_buildability_response,
    extract_production_response,
    run_buildability,
    run_production,
)
from src.api.schemas.requests import (
    BESSRequest,
    BillSavingsRequest,
    BuildabilityRequest,
    ProductionRequest,
)
from src.api.schemas.responses import (
    BESSResponse,
    BillSavingsResponse,
    BuildabilityResponse,
    ProductionResponse,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/analyses", tags=["analyses"])


@router.post("/production", response_model=ProductionResponse)
def run_production_analysis(request: ProductionRequest) -> ProductionResponse:
    """Run a production-only solar analysis.

    Accepts a ProductionRequest, runs the PySAM pipeline, and returns
    production metrics, loss breakdown, and monthly generation.
    """
    try:
        site_config = production_request_to_site_config(request)
        output_dir = Path("outputs/api") / site_config.run_name
        output_dir.mkdir(parents=True, exist_ok=True)

        results = run_production(site_config, output_dir)
        response = extract_production_response(
            results, site_config.run_name, output_dir
        )

        # Persist response JSON for future GET retrieval
        results_path = output_dir / "results.json"
        results_path.write_text(
            response.model_dump_json(indent=2), encoding="utf-8"
        )
        logger.info(f"Results saved to {results_path}")

        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Pipeline error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/bill-savings", response_model=BillSavingsResponse)
def run_bill_savings_analysis(
    request: BillSavingsRequest,
) -> BillSavingsResponse:
    """Run a production + bill savings analysis.

    Accepts a BillSavingsRequest with bill configuration, runs the PySAM
    pipeline with bill_calculation enabled, and returns production metrics,
    loss breakdown, monthly generation, and bill savings.
    """
    try:
        site_config = bill_savings_request_to_site_config(request)
        output_dir = Path("outputs/api") / site_config.run_name
        output_dir.mkdir(parents=True, exist_ok=True)

        results = run_production(site_config, output_dir)
        response = extract_bill_savings_response(
            results, site_config.run_name, output_dir
        )

        # Persist response JSON for future GET retrieval
        results_path = output_dir / "results.json"
        results_path.write_text(
            response.model_dump_json(indent=2), encoding="utf-8"
        )
        logger.info(f"Results saved to {results_path}")

        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Pipeline error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/bess", response_model=BESSResponse)
def run_bess_analysis(request: BESSRequest) -> BESSResponse:
    """Run a BESS dispatch or sizing optimization analysis.

    Handles three modes based on request content:
    - BTM single dispatch: bess config with bill, no ftm
    - FTM dispatch: ftm.dispatch_mode == "ftm"
    - Sizing optimization: bess_economics.optimize == True
    """
    try:
        site_config = bess_request_to_site_config(request)
        output_dir = Path("outputs/api") / site_config.run_name
        output_dir.mkdir(parents=True, exist_ok=True)

        is_ftm = (
            request.ftm is not None and request.ftm.dispatch_mode == "ftm"
        )
        is_optimization = (
            request.bess_economics is not None
            and request.bess_economics.optimize
        )

        results = run_production(site_config, output_dir)
        response = extract_bess_response(
            results, site_config.run_name, output_dir, is_ftm, is_optimization
        )

        # Persist response JSON for future GET retrieval
        results_path = output_dir / "results.json"
        results_path.write_text(
            response.model_dump_json(indent=2), encoding="utf-8"
        )
        logger.info(f"Results saved to {results_path}")

        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Pipeline error: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/buildability", response_model=BuildabilityResponse)
def run_buildability_analysis_endpoint(
    request: BuildabilityRequest,
) -> BuildabilityResponse:
    """Run a buildable land assessment.

    Fetches NLCD land cover and 3DEP elevation data for the site location,
    classifies terrain buildability, and returns acreage/slope metrics.
    This is a standalone analysis — no PySAM simulation is involved.
    """
    try:
        site_config = buildability_request_to_site_config(request)

        result = run_buildability(site_config)
        response = extract_buildability_response(result, site_config.run_name)

        # Persist response JSON for future GET retrieval
        output_dir = Path("outputs/api") / site_config.run_name
        output_dir.mkdir(parents=True, exist_ok=True)
        results_path = output_dir / "results.json"
        results_path.write_text(
            response.model_dump_json(indent=2), encoding="utf-8"
        )
        logger.info(f"Results saved to {results_path}")

        return response

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Buildability error: {exc}", exc_info=True)
        # Buildability calls external APIs (NLCD, 3DEP) — return 502
        # for external service failures
        raise HTTPException(
            status_code=502,
            detail=f"Buildability analysis failed: {exc}",
        ) from exc
