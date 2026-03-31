"""Domain defaults for solar system design, losses, and BESS parameters."""

SYSTEM_DEFAULTS = {
    "tracker": {
        "gcr": 0.34,
        "tilt": 60,       # rotation limit, NOT literal tilt
        "dc_ac_ratio": 1.3,
        "module_orientation": "portrait",
        "num_modules": 1,
        "ground_clearance_m": 1.5,
    },
    "fixed": {
        "gcr": 0.50,
        "tilt": 25,
        "dc_ac_ratio": 1.2,
        "module_orientation": "portrait",
        "num_modules": 2,
        "ground_clearance_m": 1.0,
    },
    "losses": {
        "shading_pct": 0.0,
        "dc_wiring_pct": 2.0,
        "ac_wiring_pct": 0.5,
        "transformer_pct": 1.0,
        "degradation_pct": 0.5,
        "availability_pct": 2.5,
        "mismatch_pct": 2.0,
        "lid_pct": 1.5,
    },
    "bess": {
        "rte_pct": 88.0,
        "min_soc_pct": 10.0,
        "max_soc_pct": 90.0,
        "installed_cost_per_kwh": 275.0,
        "cycles_warranty": 5000,
        "strategy": "global",
    },
    "bess_economics": {
        "discount_rate_pct": 7.0,
        "project_lifetime_years": 25,
        "rate_escalation_pct": 2.0,
        "duration_min_hr": 2.0,
        "duration_max_hr": 5.0,
    },
}
