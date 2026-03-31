"""OpenAI function/tool schemas for the 12 analysis API tools."""

TOOL_DEFINITIONS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "health_check",
            "description": (
                "Check if the solar modeling API is online and responsive. "
                "Call this if any other tool returns a connection error."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_modules",
            "description": (
                "Search the CEC module database (~20,000 modules) by manufacturer "
                "or model name. Returns exact CEC-listed names that can be used in "
                "the 'module' field of analysis requests. Always use this to find "
                "the correct module string before running an analysis."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "search": {
                        "type": "string",
                        "description": (
                            "Case-insensitive search string. Examples: "
                            "'Canadian Solar 400', 'LONGi bifacial', "
                            "'First Solar Series 6'"
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_inverters",
            "description": (
                "Search the CEC inverter database (~2,000 inverters) by "
                "manufacturer or model name. Returns exact CEC-listed names "
                "that can be used in the 'inverter' field of analysis requests."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "search": {
                        "type": "string",
                        "description": (
                            "Case-insensitive search string. Examples: "
                            "'SMA Sunny', 'Power Electronics', 'Sungrow SG'"
                        ),
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_load_types",
            "description": (
                "List all available DOE reference building types for load "
                "profile modeling. Use this when the user needs to select a "
                "building type for bill savings analysis but isn't sure "
                "what's available."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "build_rate",
            "description": (
                "Build, validate, and optionally save a utility rate schedule. "
                "Use this when the user describes a rate verbally or when you "
                "need to construct a rate for bill savings analysis. The rate "
                "follows URDB-compatible format with 12x24 schedule matrices. "
                "Returns the validated rate object and optionally saves to disk "
                "for reuse."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "rate": {
                        "type": "object",
                        "description": "Complete rate schedule object",
                        "properties": {
                            "utility_name": {
                                "type": "string",
                                "description": "Utility name",
                            },
                            "tariff_name": {
                                "type": "string",
                                "description": "Tariff identifier",
                            },
                            "sector": {
                                "type": "string",
                                "enum": ["commercial", "residential", "industrial"],
                                "description": "Rate sector (default: 'commercial')",
                            },
                            "fixed_charges": {
                                "type": "object",
                                "properties": {
                                    "fixed_charge_first_meter": {
                                        "type": "number",
                                        "description": "Fixed charge amount (default: 0.0)",
                                    },
                                    "fixed_charge_units": {
                                        "type": "string",
                                        "enum": ["$/month", "$/day", "$/year"],
                                        "description": "Units (default: '$/month')",
                                    },
                                },
                            },
                            "energyratestructure": {
                                "type": "array",
                                "description": (
                                    "Energy rate periods. Each period is an array of "
                                    "tier objects with {rate, max, adj}. Example for "
                                    "2 periods: [[{rate: 0.08}], [{rate: 0.15}]]"
                                ),
                                "items": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "rate": {
                                                "type": "number",
                                                "description": "Price $/kWh",
                                            },
                                            "max": {
                                                "type": "number",
                                                "description": (
                                                    "Tier ceiling kWh (omit for unlimited)"
                                                ),
                                            },
                                            "adj": {
                                                "type": "number",
                                                "description": (
                                                    "Adjustment adder $/kWh (default: 0)"
                                                ),
                                            },
                                        },
                                        "required": ["rate"],
                                    },
                                },
                            },
                            "energyweekdayschedule": {
                                "type": "array",
                                "description": (
                                    "12x24 matrix. Row = month (Jan-Dec), column = "
                                    "hour (0-23). Values are 0-indexed period indices "
                                    "into energyratestructure."
                                ),
                                "items": {
                                    "type": "array",
                                    "items": {"type": "integer"},
                                },
                            },
                            "energyweekendschedule": {
                                "type": "array",
                                "description": (
                                    "12x24 weekend schedule. Same format as weekday."
                                ),
                                "items": {
                                    "type": "array",
                                    "items": {"type": "integer"},
                                },
                            },
                            "demandratestructure": {
                                "type": "array",
                                "description": (
                                    "TOU demand periods. Each period is array of tier "
                                    "objects with {rate, max}. All 3 demand fields "
                                    "required together."
                                ),
                                "items": {
                                    "type": "array",
                                    "items": {"type": "object"},
                                },
                            },
                            "demandweekdayschedule": {
                                "type": "array",
                                "description": "12x24 TOU demand weekday schedule",
                                "items": {
                                    "type": "array",
                                    "items": {"type": "integer"},
                                },
                            },
                            "demandweekendschedule": {
                                "type": "array",
                                "description": "12x24 TOU demand weekend schedule",
                                "items": {
                                    "type": "array",
                                    "items": {"type": "integer"},
                                },
                            },
                            "flatdemandstructure": {
                                "type": "array",
                                "description": (
                                    "Flat (non-TOU) demand periods. Both flat demand "
                                    "fields required together."
                                ),
                                "items": {
                                    "type": "array",
                                    "items": {"type": "object"},
                                },
                            },
                            "flatdemandmonths": {
                                "type": "array",
                                "description": (
                                    "12-element array mapping each month to a flat "
                                    "demand period index"
                                ),
                                "items": {"type": "integer"},
                            },
                            "net_metering": {
                                "type": "object",
                                "description": "NEM configuration",
                                "properties": {
                                    "mode": {
                                        "type": "string",
                                        "enum": [
                                            "none",
                                            "flat_rate",
                                            "match_import",
                                            "detailed",
                                        ],
                                        "description": (
                                            "Export credit mode (default: 'none')"
                                        ),
                                    },
                                    "export_rate": {
                                        "type": "number",
                                        "description": (
                                            "Fixed $/kWh export credit. Required "
                                            "for flat_rate mode."
                                        ),
                                    },
                                    "export_schedule": {
                                        "type": "array",
                                        "description": (
                                            "12x24 weekday export schedule. "
                                            "Required for detailed mode."
                                        ),
                                    },
                                    "export_weekend_schedule": {
                                        "type": "array",
                                        "description": (
                                            "12x24 weekend export schedule. "
                                            "Required for detailed mode."
                                        ),
                                    },
                                    "export_rate_structure": {
                                        "type": "array",
                                        "description": (
                                            "Export rate tiers per period. "
                                            "Required for detailed mode."
                                        ),
                                    },
                                    "true_up_rate": {
                                        "type": "number",
                                        "description": (
                                            "Year-end cashout $/kWh (default: 0.0)"
                                        ),
                                    },
                                },
                            },
                        },
                        "required": [
                            "utility_name",
                            "tariff_name",
                            "energyratestructure",
                            "energyweekdayschedule",
                            "energyweekendschedule",
                        ],
                    },
                    "save_to_disk": {
                        "type": "boolean",
                        "description": (
                            "Save the validated rate to the server for reuse "
                            "(default: false)"
                        ),
                    },
                },
                "required": ["rate"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_production",
            "description": (
                "Run a production-only solar simulation using PySAM Pvsamv1. "
                "Returns annual energy (MWh), capacity factors, performance "
                "ratio, monthly production profile, and a detailed loss "
                "breakdown. This is the foundational analysis — run it before "
                "bill savings or BESS analysis."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "site": {
                        "type": "object",
                        "description": "Site location and identity",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Site name",
                            },
                            "customer": {
                                "type": "string",
                                "description": "Customer name (default: 'API')",
                            },
                            "latitude": {
                                "type": "number",
                                "description": "Latitude (-90 to 90)",
                            },
                            "longitude": {
                                "type": "number",
                                "description": "Longitude (-180 to 180)",
                            },
                            "run_name": {
                                "type": "string",
                                "description": (
                                    "Custom run identifier (auto-generated if omitted)"
                                ),
                            },
                        },
                        "required": ["name", "latitude", "longitude"],
                    },
                    "system": {
                        "type": "object",
                        "description": "PV system design parameters",
                        "properties": {
                            "dc_capacity_mw": {
                                "type": "number",
                                "description": "DC nameplate capacity in MW (>0)",
                            },
                            "ac_capacity_mw": {
                                "type": "number",
                                "description": "AC inverter capacity in MW (>0)",
                            },
                            "ac_poi_mw": {
                                "type": "number",
                                "description": (
                                    "Point of interconnection limit in MW "
                                    "(defaults to ac_capacity_mw)"
                                ),
                            },
                            "module": {
                                "type": "string",
                                "description": (
                                    "Exact CEC module name from equipment search"
                                ),
                            },
                            "inverter": {
                                "type": "string",
                                "description": (
                                    "Exact CEC inverter name from equipment search"
                                ),
                            },
                            "racking": {
                                "type": "string",
                                "enum": ["fixed", "tracker"],
                                "description": "'fixed' or 'tracker'",
                            },
                            "tilt": {
                                "type": "number",
                                "description": (
                                    "Tilt angle 0-90. For trackers, this is the "
                                    "rotation limit (default: 60). For fixed, this "
                                    "is the tilt angle (default: 25)."
                                ),
                            },
                            "azimuth": {
                                "type": "number",
                                "description": (
                                    "Azimuth 0-360 (default: 180 = due south)"
                                ),
                            },
                            "module_orientation": {
                                "type": "string",
                                "enum": ["portrait", "landscape"],
                                "description": (
                                    "Module orientation (default: 'portrait')"
                                ),
                            },
                            "num_modules": {
                                "type": "integer",
                                "description": (
                                    "Modules per rack in height, 1 or 2 (default: 1). "
                                    "NOT total site module count."
                                ),
                            },
                            "ground_clearance_m": {
                                "type": "number",
                                "description": (
                                    "Ground clearance in meters (default: 1.5)"
                                ),
                            },
                            "bifacial": {
                                "type": "boolean",
                                "description": "Bifacial modules (default: false)",
                            },
                            "gcr": {
                                "type": "number",
                                "description": (
                                    "Ground coverage ratio 0 to 1 exclusive. "
                                    "Typical: 0.30-0.35 for trackers, 0.45-0.55 "
                                    "for fixed-tilt."
                                ),
                            },
                        },
                        "required": [
                            "dc_capacity_mw",
                            "ac_capacity_mw",
                            "module",
                            "inverter",
                            "racking",
                            "gcr",
                        ],
                    },
                    "losses": {
                        "type": "object",
                        "description": (
                            "System loss overrides. All fields optional; API "
                            "applies defaults if omitted."
                        ),
                        "properties": {
                            "shading_pct": {
                                "type": "number",
                                "description": "Shading loss % (default: 0.0)",
                            },
                            "dc_wiring_pct": {
                                "type": "number",
                                "description": "DC wiring loss % (default: 2.0)",
                            },
                            "ac_wiring_pct": {
                                "type": "number",
                                "description": "AC wiring loss % (default: 0.5)",
                            },
                            "transformer_pct": {
                                "type": "number",
                                "description": "Transformer loss % (default: 1.0)",
                            },
                            "degradation_pct": {
                                "type": "number",
                                "description": "Annual degradation % (default: 0.5)",
                            },
                            "availability_pct": {
                                "type": "number",
                                "description": "Availability loss % (default: 2.5)",
                            },
                            "mismatch_pct": {
                                "type": "number",
                                "description": "Module mismatch % (default: 2.0)",
                            },
                            "lid_pct": {
                                "type": "number",
                                "description": (
                                    "Light-induced degradation % (default: 1.5)"
                                ),
                            },
                        },
                    },
                },
                "required": ["site", "system"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_bill_savings",
            "description": (
                "Run solar production modeling plus utility bill savings "
                "analysis. Requires a rate schedule (inline, OpenEI lookup, "
                "or uploaded file) and a load profile (DOE building type or "
                "uploaded 8760 CSV). Returns everything from run_production "
                "plus annual/monthly bill savings, demand charge reduction, "
                "NEM export credits, and avoided cost per kWh."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "site": {
                        "type": "object",
                        "description": (
                            "Same as run_production site object "
                            "(name, latitude, longitude required)"
                        ),
                    },
                    "system": {
                        "type": "object",
                        "description": (
                            "Same as run_production system object (all fields)"
                        ),
                    },
                    "losses": {
                        "type": "object",
                        "description": (
                            "Same as run_production losses object "
                            "(all fields optional)"
                        ),
                    },
                    "bill": {
                        "type": "object",
                        "description": (
                            "Bill calculation configuration. Requires exactly "
                            "one rate source and exactly one load source."
                        ),
                        "properties": {
                            "rate": {
                                "type": "object",
                                "description": (
                                    "Inline rate schedule object. Use the "
                                    "build_rate tool to construct and validate, "
                                    "then pass the returned rate object here "
                                    "directly."
                                ),
                            },
                            "rate_file_path": {
                                "type": "string",
                                "description": (
                                    "Server-side path to uploaded rate JSON file"
                                ),
                            },
                            "utility_name": {
                                "type": "string",
                                "description": (
                                    "OpenEI utility name (must pair with tariff_name)"
                                ),
                            },
                            "tariff_name": {
                                "type": "string",
                                "description": (
                                    "OpenEI tariff name (must pair with utility_name)"
                                ),
                            },
                            "load_profile_path": {
                                "type": "string",
                                "description": (
                                    "Server-side path to uploaded 8760 load CSV"
                                ),
                            },
                            "load_type": {
                                "type": "string",
                                "description": (
                                    "DOE building type (e.g., 'SmallOffice', "
                                    "'Hospital'). Must pair with "
                                    "annual_consumption_kwh."
                                ),
                            },
                            "annual_consumption_kwh": {
                                "type": "number",
                                "description": (
                                    "Annual consumption for scaling the typical "
                                    "load profile. Required when using load_type."
                                ),
                            },
                            "peak_demand_kw": {
                                "type": "number",
                                "description": (
                                    "Peak demand for profile scaling (optional)"
                                ),
                            },
                        },
                    },
                },
                "required": ["site", "system", "bill"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_bess",
            "description": (
                "Run BESS analysis. Supports three modes: (1) BTM dispatch — "
                "optimizes battery against utility bill; (2) FTM wholesale "
                "dispatch — optimizes against ISO LMP prices (PJM, ERCOT, "
                "CAISO); (3) Sizing optimization — sweeps power/duration "
                "combinations to find NPV-optimal BESS size. BTM mode requires "
                "a rate schedule and load profile. FTM mode auto-detects ISO "
                "from lat/lon with manual override."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "site": {
                        "type": "object",
                        "description": (
                            "Same as run_production site object "
                            "(name, latitude, longitude required)"
                        ),
                    },
                    "system": {
                        "type": "object",
                        "description": (
                            "Same as run_production system object (all fields)"
                        ),
                    },
                    "losses": {
                        "type": "object",
                        "description": (
                            "Same as run_production losses object "
                            "(all fields optional)"
                        ),
                    },
                    "bill": {
                        "type": "object",
                        "description": (
                            "Bill configuration. Required for BTM mode; "
                            "optional for FTM mode."
                        ),
                    },
                    "bess": {
                        "type": "object",
                        "description": "Battery configuration",
                        "properties": {
                            "power_mw": {
                                "type": "number",
                                "description": "Battery power rating in MW",
                            },
                            "duration_hr": {
                                "type": "number",
                                "description": "Storage duration in hours",
                            },
                            "rte_pct": {
                                "type": "number",
                                "description": (
                                    "Round-trip efficiency % (default: 88.0)"
                                ),
                            },
                            "min_soc_pct": {
                                "type": "number",
                                "description": (
                                    "Minimum state of charge % (default: 10.0)"
                                ),
                            },
                            "max_soc_pct": {
                                "type": "number",
                                "description": (
                                    "Maximum state of charge % (default: 90.0)"
                                ),
                            },
                            "strategy": {
                                "type": "string",
                                "enum": [
                                    "global",
                                    "peak_shaving",
                                    "tou_arbitrage",
                                ],
                                "description": (
                                    "Dispatch strategy (default: 'global')"
                                ),
                            },
                            "installed_cost_per_kwh": {
                                "type": "number",
                                "description": (
                                    "BESS installed cost $/kWh (default: 275.0)"
                                ),
                            },
                            "cycles_warranty": {
                                "type": "integer",
                                "description": (
                                    "Warranted cycle count (default: 5000)"
                                ),
                            },
                            "solar_only_charging": {
                                "type": "boolean",
                                "description": (
                                    "ITC constraint: battery charges only from "
                                    "solar (default: false). Mutually exclusive "
                                    "with grid_only_charging."
                                ),
                            },
                            "grid_only_charging": {
                                "type": "boolean",
                                "description": (
                                    "Standalone mode: battery charges only from "
                                    "grid (default: false). Mutually exclusive "
                                    "with solar_only_charging."
                                ),
                            },
                        },
                    },
                    "ftm": {
                        "type": "object",
                        "description": (
                            "Front-of-meter configuration. Include this object "
                            "to run FTM wholesale dispatch."
                        ),
                        "properties": {
                            "dispatch_mode": {
                                "type": "string",
                                "enum": ["btm", "ftm"],
                                "description": "'btm' (default) or 'ftm'",
                            },
                            "iso": {
                                "type": "string",
                                "enum": ["pjm", "ercot", "caiso"],
                                "description": (
                                    "ISO market. Auto-detected from lat/lon "
                                    "if omitted."
                                ),
                            },
                            "lmp_zone": {
                                "type": "string",
                                "description": (
                                    "Pricing zone override (ISO-specific)"
                                ),
                            },
                            "lmp_market": {
                                "type": "string",
                                "enum": [
                                    "DAY_AHEAD_HOURLY",
                                    "REAL_TIME_HOURLY",
                                ],
                                "description": (
                                    "Market type (default: 'DAY_AHEAD_HOURLY')"
                                ),
                            },
                            "lmp_year": {
                                "type": "integer",
                                "description": (
                                    "Calendar year for historical LMP data"
                                ),
                            },
                            "ancillary_revenue_per_kw_year": {
                                "type": "number",
                                "description": (
                                    "Ancillary services revenue $/kW/yr "
                                    "(default: 0.0)"
                                ),
                            },
                        },
                    },
                    "bess_economics": {
                        "type": "object",
                        "description": (
                            "Economics parameters for NPV calculation and "
                            "optional sizing optimization."
                        ),
                        "properties": {
                            "optimize": {
                                "type": "boolean",
                                "description": (
                                    "Enable sizing sweep (default: false). "
                                    "When true, the API sweeps power/duration "
                                    "combinations and returns the NPV-optimal "
                                    "size."
                                ),
                            },
                            "discount_rate_pct": {
                                "type": "number",
                                "description": "Discount rate % (default: 7.0)",
                            },
                            "project_lifetime_years": {
                                "type": "integer",
                                "description": (
                                    "Project lifetime in years (default: 25)"
                                ),
                            },
                            "rate_escalation_pct": {
                                "type": "number",
                                "description": (
                                    "Annual rate escalation % (default: 2.0)"
                                ),
                            },
                            "solar_cost_per_kw_dc": {
                                "type": "number",
                                "description": (
                                    "Solar installed cost $/kW_DC. Required "
                                    "when optimize=true and not grid-only."
                                ),
                            },
                            "solar_cost_per_kw_ac": {
                                "type": "number",
                                "description": (
                                    "Solar installed cost $/kW_AC. Required "
                                    "when optimize=true and not grid-only."
                                ),
                            },
                            "solar_opex_per_kw_dc_year": {
                                "type": "number",
                                "description": (
                                    "Solar O&M $/kW_DC/yr (default: 0.0)"
                                ),
                            },
                            "bess_opex_per_kw_year": {
                                "type": "number",
                                "description": (
                                    "BESS O&M $/kW/yr (default: 0.0)"
                                ),
                            },
                            "power_min_mw": {
                                "type": "number",
                                "description": (
                                    "Minimum power for sizing sweep"
                                ),
                            },
                            "power_max_mw": {
                                "type": "number",
                                "description": (
                                    "Maximum power for sizing sweep"
                                ),
                            },
                            "duration_min_hr": {
                                "type": "number",
                                "description": (
                                    "Minimum duration for sweep (default: 2.0)"
                                ),
                            },
                            "duration_max_hr": {
                                "type": "number",
                                "description": (
                                    "Maximum duration for sweep (default: 5.0)"
                                ),
                            },
                        },
                    },
                },
                "required": ["site", "system", "bess"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_buildability",
            "description": (
                "Assess buildable land area using NLCD 2021 land cover "
                "classification and USGS 3DEP slope analysis. Does NOT run "
                "a PV simulation. Returns buildable/excluded acreage, land "
                "cover breakdown by NLCD class, slope statistics with "
                "distribution, and percentage of area suitable for tracker "
                "vs fixed-tilt. Specify either a KMZ polygon boundary or an "
                "analysis radius around the site coordinates."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "site": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "latitude": {"type": "number"},
                            "longitude": {"type": "number"},
                        },
                        "required": ["name", "latitude", "longitude"],
                    },
                    "buildability": {
                        "type": "object",
                        "properties": {
                            "kmz_file_path": {
                                "type": "string",
                                "description": (
                                    "Server-side path to uploaded KMZ file. "
                                    "Mutually exclusive with analysis_radius_km."
                                ),
                            },
                            "analysis_radius_km": {
                                "type": "number",
                                "description": (
                                    "Analysis radius in km. Mutually exclusive "
                                    "with kmz_file_path. Good for initial "
                                    "screening (try 1-2 km)."
                                ),
                            },
                        },
                    },
                    "include_maps": {
                        "type": "boolean",
                        "description": (
                            "Generate PNG map figures (default: false)"
                        ),
                    },
                },
                "required": ["site"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_results",
            "description": (
                "Retrieve the full results JSON from a previously completed "
                "analysis run. Use this to re-examine results without "
                "re-running the analysis."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": (
                            "Run identifier from a previous analysis response"
                        ),
                    },
                },
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_report",
            "description": (
                "Download the PDF report generated for a completed analysis "
                "run. Returns the report as a PDF file."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "Run identifier",
                    },
                },
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_timeseries",
            "description": (
                "Download the 8760 hourly timeseries CSV for a completed "
                "production or bill savings run. Contains hourly AC output, "
                "POA irradiance, and other simulation outputs."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "Run identifier",
                    },
                },
                "required": ["run_id"],
            },
        },
    },
]

TOOL_ENDPOINTS: dict[str, tuple[str, str]] = {
    "health_check": ("GET", "/health"),
    "search_modules": ("GET", "/analyses/equipment/modules"),
    "search_inverters": ("GET", "/analyses/equipment/inverters"),
    "list_load_types": ("GET", "/analyses/load-types"),
    "build_rate": ("POST", "/rates/build"),
    "run_production": ("POST", "/analyses/production"),
    "run_bill_savings": ("POST", "/analyses/bill-savings"),
    "run_bess": ("POST", "/analyses/bess"),
    "run_buildability": ("POST", "/analyses/buildability"),
    "get_results": ("GET", "/analyses/{run_id}/results"),
    "get_report": ("GET", "/analyses/{run_id}/report"),
    "get_timeseries": ("GET", "/analyses/{run_id}/timeseries"),
}
