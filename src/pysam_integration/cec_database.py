"""CEC Database integration — loads real CEC CSV databases (~25k modules, ~4k inverters)."""

from dataclasses import dataclass
from difflib import get_close_matches
from pathlib import Path
from typing import Optional

import pandas as pd

from src.pysam_integration.exceptions import InverterNotFoundError, ModuleNotFoundError
from src.utils.logger import setup_logger

# Default paths to CEC CSV databases (relative to project root)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_MODULES_CSV = _PROJECT_ROOT / "docs" / "CEC Modules.csv"
DEFAULT_INVERTERS_CSV = _PROJECT_ROOT / "docs" / "CEC Inverters.csv"


@dataclass
class CECModuleParams:
    """CEC module parameters for the five-parameter single-diode model."""

    name: str
    area: float  # Module area (m²)
    pmax: float  # Max power at STC (W)
    vmp: float  # Voltage at max power (V)
    imp: float  # Current at max power (A)
    voc: float  # Open circuit voltage (V)
    isc: float  # Short circuit current (A)

    # Five-parameter single-diode model fields
    n_s: int  # Number of cells in series
    alpha_sc: float  # Short-circuit current temperature coefficient (A/K)
    beta_oc: float  # Open-circuit voltage temperature coefficient (V/K)
    a_ref: float  # Modified ideality factor at reference conditions (V)
    i_l_ref: float  # Light current at reference conditions (A)
    i_o_ref: float  # Diode saturation current at reference conditions (A)
    r_s: float  # Series resistance (Ohm)
    r_sh_ref: float  # Shunt resistance at reference conditions (Ohm)
    adjust: float  # Temperature coefficient adjustment (%)
    gamma_pmp: float  # Max power temperature coefficient (%/K)
    t_noct: float  # Nominal operating cell temperature (°C)

    # Physical / classification fields
    bifacial: int  # 0 or 1
    length: float  # Module length (m), 0 if not available
    width: float  # Module width (m), 0 if not available
    technology: str  # e.g. "Mono-c-Si", "Multi-c-Si"

    @property
    def efficiency(self) -> float:
        """Calculate module efficiency."""
        if self.area and self.pmax:
            return self.pmax / (self.area * 1000)
        return 0.20


@dataclass
class CECInverterParams:
    """CEC inverter parameters extracted from database."""

    name: str
    paco: float  # Max AC power output (W)
    pdco: float  # DC power at which AC power = Paco (W)
    vdco: float  # DC voltage at which AC power = Paco (V)
    pso: float  # DC power required to start inverter (W)
    vdcmax: float  # Max DC voltage (V)
    mppt_low: float  # Min MPPT voltage (V)
    mppt_high: float  # Max MPPT voltage (V)
    c0: float  # Sandia coefficient: Paco correction for DC power
    c1: float  # Sandia coefficient: Pso correction for DC voltage
    c2: float  # Sandia coefficient: Paco correction for DC voltage
    c3: float  # Sandia coefficient: Pso correction for DC voltage
    pnt: float  # Night tare loss (W)
    vac: float  # AC voltage (V)
    idcmax: float  # Max DC current (A)

    @property
    def efficiency(self) -> float:
        """Calculate inverter CEC efficiency as percentage (Paco/Pdco * 100)."""
        if self.pdco:
            return (self.paco / self.pdco) * 100
        return 96.0


class CECDatabase:
    """Interface to CEC module and inverter databases loaded from CSV files."""

    def __init__(
        self,
        modules_csv_path: Path | None = None,
        inverters_csv_path: Path | None = None,
    ) -> None:
        self.logger = setup_logger(__name__)

        modules_path = modules_csv_path or DEFAULT_MODULES_CSV
        inverters_path = inverters_csv_path or DEFAULT_INVERTERS_CSV

        self.module_db = self._load_modules_csv(modules_path)
        self.inverter_db = self._load_inverters_csv(inverters_path)

        self.logger.info(
            f"Loaded {len(self.module_db)} modules from {modules_path.name}"
        )
        self.logger.info(
            f"Loaded {len(self.inverter_db)} inverters from {inverters_path.name}"
        )

    def _load_modules_csv(self, csv_path: Path) -> dict[str, dict]:
        """Load CEC modules CSV into a dict keyed by Name.

        CSV structure: row 1 = column names, rows 2-3 = units/PySAM mapping, row 4+ = data.
        """
        df = pd.read_csv(csv_path, header=0, skiprows=[1, 2])

        module_db: dict[str, dict] = {}
        for _, row in df.iterrows():
            name = str(row["Name"])
            module_db[name] = {
                "STC": float(row["STC"]),
                "A_c": float(row["A_c"]),
                "V_mp_ref": float(row["V_mp_ref"]),
                "I_mp_ref": float(row["I_mp_ref"]),
                "V_oc_ref": float(row["V_oc_ref"]),
                "I_sc_ref": float(row["I_sc_ref"]),
                "N_s": int(row["N_s"]),
                "alpha_sc": float(row["alpha_sc"]),
                "beta_oc": float(row["beta_oc"]),
                "a_ref": float(row["a_ref"]),
                "I_L_ref": float(row["I_L_ref"]),
                "I_o_ref": float(row["I_o_ref"]),
                "R_s": float(row["R_s"]),
                "R_sh_ref": float(row["R_sh_ref"]),
                "Adjust": float(row["Adjust"]),
                "gamma_pmp": float(row["gamma_pmp"]),
                "T_NOCT": float(row["T_NOCT"]),
                "Bifacial": int(row["Bifacial"]),
                "Length": float(row["Length"]) if pd.notna(row.get("Length")) else 0.0,
                "Width": float(row["Width"]) if pd.notna(row.get("Width")) else 0.0,
                "Technology": str(row["Technology"]),
            }

        return module_db

    def _load_inverters_csv(self, csv_path: Path) -> dict[str, dict]:
        """Load CEC inverters CSV into a dict keyed by Name.

        CSV structure: row 1 = column names, rows 2-3 = units/PySAM mapping, row 4+ = data.
        """
        df = pd.read_csv(csv_path, header=0, skiprows=[1, 2])

        inverter_db: dict[str, dict] = {}
        for _, row in df.iterrows():
            name = str(row["Name"])
            inverter_db[name] = {
                "Paco": float(row["Paco"]),
                "Pdco": float(row["Pdco"]),
                "Vdco": float(row["Vdco"]),
                "Pso": float(row["Pso"]),
                "Vdcmax": float(row["Vdcmax"]),
                "Mppt_low": float(row["Mppt_low"]),
                "Mppt_high": float(row["Mppt_high"]),
                "C0": float(row["C0"]),
                "C1": float(row["C1"]),
                "C2": float(row["C2"]),
                "C3": float(row["C3"]),
                "Pnt": float(row["Pnt"]),
                "Vac": float(row["Vac"]),
                "Idcmax": float(row["Idcmax"]),
            }

        return inverter_db

    def get_module_params(self, module_name: str) -> CECModuleParams:
        """Retrieve CEC module parameters by name.

        Args:
            module_name: Exact module model name from CSV.

        Returns:
            CECModuleParams with all parameters.

        Raises:
            ModuleNotFoundError: If module name not found in database.
        """
        if module_name not in self.module_db:
            similar = get_close_matches(
                module_name, self.module_db.keys(), n=5, cutoff=0.6
            )
            raise ModuleNotFoundError(module_name, similar)

        data = self.module_db[module_name]

        return CECModuleParams(
            name=module_name,
            area=data["A_c"],
            pmax=data["STC"],
            vmp=data["V_mp_ref"],
            imp=data["I_mp_ref"],
            voc=data["V_oc_ref"],
            isc=data["I_sc_ref"],
            n_s=data["N_s"],
            alpha_sc=data["alpha_sc"],
            beta_oc=data["beta_oc"],
            a_ref=data["a_ref"],
            i_l_ref=data["I_L_ref"],
            i_o_ref=data["I_o_ref"],
            r_s=data["R_s"],
            r_sh_ref=data["R_sh_ref"],
            adjust=data["Adjust"],
            gamma_pmp=data["gamma_pmp"],
            t_noct=data["T_NOCT"],
            bifacial=data["Bifacial"],
            length=data["Length"],
            width=data["Width"],
            technology=data["Technology"],
        )

    def get_inverter_params(self, inverter_name: str) -> CECInverterParams:
        """Retrieve CEC inverter parameters by name.

        Args:
            inverter_name: Exact inverter model name from CSV.

        Returns:
            CECInverterParams with all parameters.

        Raises:
            InverterNotFoundError: If inverter name not found in database.
        """
        if inverter_name not in self.inverter_db:
            similar = get_close_matches(
                inverter_name, self.inverter_db.keys(), n=5, cutoff=0.6
            )
            raise InverterNotFoundError(inverter_name, similar)

        data = self.inverter_db[inverter_name]

        return CECInverterParams(
            name=inverter_name,
            paco=data["Paco"],
            pdco=data["Pdco"],
            vdco=data["Vdco"],
            pso=data["Pso"],
            vdcmax=data["Vdcmax"],
            mppt_low=data["Mppt_low"],
            mppt_high=data["Mppt_high"],
            c0=data["C0"],
            c1=data["C1"],
            c2=data["C2"],
            c3=data["C3"],
            pnt=data["Pnt"],
            vac=data["Vac"],
            idcmax=data["Idcmax"],
        )

    def list_modules(
        self,
        search_term: Optional[str] = None,
        min_stc: Optional[float] = None,
        max_stc: Optional[float] = None,
    ) -> list[str]:
        """List available module names, optionally filtered by search and STC range.

        Args:
            search_term: Space-separated tokens; a module matches if ALL tokens
                appear as case-insensitive substrings in its name.
            min_stc: Minimum STC power (W) filter.
            max_stc: Maximum STC power (W) filter.
        """
        results: list[str] = []

        tokens = search_term.lower().split() if search_term else []

        for name, data in self.module_db.items():
            # Text filter: every token must appear in the name
            if tokens:
                name_lower = name.lower()
                if not all(tok in name_lower for tok in tokens):
                    continue

            # Numeric STC filters
            if min_stc is not None and data["STC"] < min_stc:
                continue
            if max_stc is not None and data["STC"] > max_stc:
                continue

            results.append(name)

        return results

    def list_inverters(
        self,
        search_term: Optional[str] = None,
        min_paco: Optional[float] = None,
        max_paco: Optional[float] = None,
    ) -> list[str]:
        """List available inverter names, optionally filtered by search and Paco range.

        Args:
            search_term: Space-separated tokens; an inverter matches if ALL tokens
                appear as case-insensitive substrings in its name.
            min_paco: Minimum rated AC power (W) filter.
            max_paco: Maximum rated AC power (W) filter.
        """
        results: list[str] = []

        tokens = search_term.lower().split() if search_term else []

        for name, data in self.inverter_db.items():
            # Text filter: every token must appear in the name
            if tokens:
                name_lower = name.lower()
                if not all(tok in name_lower for tok in tokens):
                    continue

            # Numeric Paco filters
            if min_paco is not None and data["Paco"] < min_paco:
                continue
            if max_paco is not None and data["Paco"] > max_paco:
                continue

            results.append(name)

        return results
