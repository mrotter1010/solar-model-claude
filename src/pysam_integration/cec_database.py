"""CEC Database integration with hardcoded sample data for MVP."""

from dataclasses import dataclass
from difflib import get_close_matches
from typing import Optional

from src.pysam_integration.exceptions import InverterNotFoundError, ModuleNotFoundError
from src.utils.logger import setup_logger

# Hardcoded sample CEC module database (20 realistic modules)
CEC_MODULES_DB: dict[str, dict[str, float | int]] = {
    "Canadian Solar CS3U-355P": {
        "Vintage": 2019,
        "Area": 1.94,
        "Pmax": 355.0,
        "Vmp": 39.1,
        "Imp": 9.08,
        "Voc": 46.8,
        "Isc": 9.68,
    },
    "Canadian Solar CS1U-410MS": {
        "Vintage": 2021,
        "Area": 2.01,
        "Pmax": 410.0,
        "Vmp": 40.6,
        "Imp": 10.1,
        "Voc": 49.1,
        "Isc": 10.7,
    },
    "Jinko Solar JKM400M-72HL-BDV": {
        "Vintage": 2020,
        "Area": 2.01,
        "Pmax": 400.0,
        "Vmp": 41.5,
        "Imp": 9.64,
        "Voc": 49.9,
        "Isc": 10.18,
    },
    "Trina Solar TSM-DEG19C.20": {
        "Vintage": 2021,
        "Area": 2.01,
        "Pmax": 485.0,
        "Vmp": 42.1,
        "Imp": 11.52,
        "Voc": 50.4,
        "Isc": 12.23,
    },
    "LONGi Solar LR5-72HIH-450M": {
        "Vintage": 2021,
        "Area": 2.14,
        "Pmax": 450.0,
        "Vmp": 41.9,
        "Imp": 10.74,
        "Voc": 50.1,
        "Isc": 11.38,
    },
    "SunPower SPR-X21-335-BLK": {
        "Vintage": 2017,
        "Area": 1.63,
        "Pmax": 335.0,
        "Vmp": 57.3,
        "Imp": 5.85,
        "Voc": 67.9,
        "Isc": 6.23,
    },
    "SunPower SPR-MAX3-400": {
        "Vintage": 2019,
        "Area": 1.69,
        "Pmax": 400.0,
        "Vmp": 67.5,
        "Imp": 5.93,
        "Voc": 80.7,
        "Isc": 6.34,
    },
    "REC Solar REC400AA": {
        "Vintage": 2020,
        "Area": 1.94,
        "Pmax": 400.0,
        "Vmp": 40.1,
        "Imp": 9.98,
        "Voc": 48.0,
        "Isc": 10.53,
    },
    "Hanwha Q CELLS Q.PEAK DUO L-G6.2 405": {
        "Vintage": 2020,
        "Area": 1.88,
        "Pmax": 405.0,
        "Vmp": 38.8,
        "Imp": 10.44,
        "Voc": 46.0,
        "Isc": 11.03,
    },
    "JA Solar JAM72S20-450/MR": {
        "Vintage": 2021,
        "Area": 2.09,
        "Pmax": 450.0,
        "Vmp": 41.6,
        "Imp": 10.82,
        "Voc": 50.4,
        "Isc": 11.45,
    },
    "First Solar FS-6440": {
        "Vintage": 2021,
        "Area": 2.01,
        "Pmax": 440.0,
        "Vmp": 142.5,
        "Imp": 3.09,
        "Voc": 171.0,
        "Isc": 3.35,
    },
    "Panasonic VBHN340SA17": {
        "Vintage": 2018,
        "Area": 1.69,
        "Pmax": 340.0,
        "Vmp": 58.0,
        "Imp": 5.86,
        "Voc": 69.7,
        "Isc": 6.24,
    },
    "Silfab Solar SIL-380 NX": {
        "Vintage": 2020,
        "Area": 1.88,
        "Pmax": 380.0,
        "Vmp": 39.5,
        "Imp": 9.62,
        "Voc": 47.5,
        "Isc": 10.17,
    },
    "Mission Solar MSE315SQ5T": {
        "Vintage": 2018,
        "Area": 1.64,
        "Pmax": 315.0,
        "Vmp": 32.9,
        "Imp": 9.58,
        "Voc": 40.0,
        "Isc": 10.13,
    },
    "Axitec AXIpremium XL HC BF 410": {
        "Vintage": 2021,
        "Area": 2.01,
        "Pmax": 410.0,
        "Vmp": 38.4,
        "Imp": 10.68,
        "Voc": 45.9,
        "Isc": 11.35,
    },
    "Risen Energy RSM144-6-390BMDG": {
        "Vintage": 2020,
        "Area": 1.95,
        "Pmax": 390.0,
        "Vmp": 40.8,
        "Imp": 9.56,
        "Voc": 49.0,
        "Isc": 10.13,
    },
    "Phono Solar PS400M4-20/VH": {
        "Vintage": 2020,
        "Area": 1.95,
        "Pmax": 400.0,
        "Vmp": 41.4,
        "Imp": 9.66,
        "Voc": 49.8,
        "Isc": 10.23,
    },
    "Vikram Solar ELDORA VSP72-5-390": {
        "Vintage": 2020,
        "Area": 1.97,
        "Pmax": 390.0,
        "Vmp": 40.5,
        "Imp": 9.63,
        "Voc": 48.9,
        "Isc": 10.19,
    },
    "Seraphim SRP-405-BMB-HV": {
        "Vintage": 2020,
        "Area": 2.01,
        "Pmax": 405.0,
        "Vmp": 37.2,
        "Imp": 10.89,
        "Voc": 44.8,
        "Isc": 11.58,
    },
    "Astronergy CHSM6612M-HC": {
        "Vintage": 2020,
        "Area": 1.98,
        "Pmax": 395.0,
        "Vmp": 40.1,
        "Imp": 9.85,
        "Voc": 48.3,
        "Isc": 10.45,
    },
}

# Hardcoded sample CEC inverter database (20 realistic inverters)
CEC_INVERTERS_DB: dict[str, dict[str, float]] = {
    "SMA America: SB5.0-1SP-US-40 240V": {
        "Paco": 5000.0,
        "Pdco": 5151.5,
        "Vdco": 310.0,
        "Pso": 10.85,
        "Vdcmax": 600.0,
        "Mppt_low": 80.0,
        "Mppt_high": 600.0,
    },
    "SMA America: SB7.7-1SP-US-40 240V": {
        "Paco": 7700.0,
        "Pdco": 7938.3,
        "Vdco": 360.0,
        "Pso": 17.2,
        "Vdcmax": 600.0,
        "Mppt_low": 80.0,
        "Mppt_high": 600.0,
    },
    "Fronius USA: Primo 8.2-1 208-240": {
        "Paco": 8200.0,
        "Pdco": 8458.4,
        "Vdco": 365.0,
        "Pso": 23.0,
        "Vdcmax": 600.0,
        "Mppt_low": 80.0,
        "Mppt_high": 600.0,
    },
    "SolarEdge Technologies Inc: SE10000A-US (240V)": {
        "Paco": 10000.0,
        "Pdco": 10300.0,
        "Vdco": 360.0,
        "Pso": 14.2,
        "Vdcmax": 600.0,
        "Mppt_low": 300.0,
        "Mppt_high": 480.0,
    },
    "Enphase Energy: IQ7PLUS-72-2-US": {
        "Paco": 295.0,
        "Pdco": 301.4,
        "Vdco": 37.0,
        "Pso": 0.02,
        "Vdcmax": 60.0,
        "Mppt_low": 16.0,
        "Mppt_high": 60.0,
    },
    "ABB: PVS-100-TL-OUTD-US 480V": {
        "Paco": 100000.0,
        "Pdco": 102564.0,
        "Vdco": 683.0,
        "Pso": 303.0,
        "Vdcmax": 1000.0,
        "Mppt_low": 570.0,
        "Mppt_high": 850.0,
    },
    "SMA America: Sunny Central 2500-EV-US (800V)": {
        "Paco": 2500000.0,
        "Pdco": 2551020.4,
        "Vdco": 811.0,
        "Pso": 5400.0,
        "Vdcmax": 1100.0,
        "Mppt_low": 580.0,
        "Mppt_high": 850.0,
    },
    "Power Electronics: FS3000E": {
        "Paco": 3000000.0,
        "Pdco": 3077524.0,
        "Vdco": 805.0,
        "Pso": 7680.0,
        "Vdcmax": 1300.0,
        "Mppt_low": 480.0,
        "Mppt_high": 820.0,
    },
    "SMA America: SB3.8-1SP-US-40 240V": {
        "Paco": 3800.0,
        "Pdco": 3916.0,
        "Vdco": 295.0,
        "Pso": 8.7,
        "Vdcmax": 600.0,
        "Mppt_low": 80.0,
        "Mppt_high": 600.0,
    },
    "SolarEdge Technologies Inc: SE5000A-US (240V)": {
        "Paco": 5000.0,
        "Pdco": 5150.0,
        "Vdco": 360.0,
        "Pso": 9.5,
        "Vdcmax": 600.0,
        "Mppt_low": 300.0,
        "Mppt_high": 480.0,
    },
    "Fronius USA: Symo 24.0-3 480": {
        "Paco": 24000.0,
        "Pdco": 24742.0,
        "Vdco": 720.0,
        "Pso": 55.0,
        "Vdcmax": 1000.0,
        "Mppt_low": 420.0,
        "Mppt_high": 800.0,
    },
    "SMA America: SB6.0-1SP-US-40 240V": {
        "Paco": 6000.0,
        "Pdco": 6186.0,
        "Vdco": 330.0,
        "Pso": 14.0,
        "Vdcmax": 600.0,
        "Mppt_low": 80.0,
        "Mppt_high": 600.0,
    },
    "Chint Power Systems: CPS SCH50KTL-DO-US-400": {
        "Paco": 50000.0,
        "Pdco": 51281.0,
        "Vdco": 730.0,
        "Pso": 150.0,
        "Vdcmax": 1000.0,
        "Mppt_low": 200.0,
        "Mppt_high": 900.0,
    },
    "Schneider Electric: Conext CL25000E-US": {
        "Paco": 25000.0,
        "Pdco": 25641.0,
        "Vdco": 625.0,
        "Pso": 80.0,
        "Vdcmax": 1000.0,
        "Mppt_low": 300.0,
        "Mppt_high": 800.0,
    },
    "Huawei Technologies Co.: SUN2000-100KTL-M1": {
        "Paco": 110000.0,
        "Pdco": 113402.0,
        "Vdco": 810.0,
        "Pso": 300.0,
        "Vdcmax": 1100.0,
        "Mppt_low": 200.0,
        "Mppt_high": 1000.0,
    },
    "Delta Products Corporation: M50A": {
        "Paco": 50000.0,
        "Pdco": 51546.0,
        "Vdco": 650.0,
        "Pso": 100.0,
        "Vdcmax": 1000.0,
        "Mppt_low": 330.0,
        "Mppt_high": 800.0,
    },
    "Sungrow Power Supply: SG110CX": {
        "Paco": 110000.0,
        "Pdco": 113402.0,
        "Vdco": 875.0,
        "Pso": 280.0,
        "Vdcmax": 1100.0,
        "Mppt_low": 520.0,
        "Mppt_high": 1000.0,
    },
    "GoodWe USA: GW30K-DT": {
        "Paco": 30000.0,
        "Pdco": 30928.0,
        "Vdco": 600.0,
        "Pso": 75.0,
        "Vdcmax": 1000.0,
        "Mppt_low": 180.0,
        "Mppt_high": 850.0,
    },
    "Ginlong Technologies: Solis-75K-EV": {
        "Paco": 75000.0,
        "Pdco": 77160.0,
        "Vdco": 700.0,
        "Pso": 200.0,
        "Vdcmax": 1100.0,
        "Mppt_low": 200.0,
        "Mppt_high": 1000.0,
    },
    "Sungrow Power Supply: SG250HX": {
        "Paco": 250000.0,
        "Pdco": 256410.0,
        "Vdco": 970.0,
        "Pso": 600.0,
        "Vdcmax": 1500.0,
        "Mppt_low": 500.0,
        "Mppt_high": 1300.0,
    },
}


@dataclass
class CECModuleParams:
    """CEC module parameters extracted from database."""

    name: str
    area: float  # Module area (m²)
    pmax: float  # Max power (W)
    vmp: float  # Voltage at max power (V)
    imp: float  # Current at max power (A)
    voc: float  # Open circuit voltage (V)
    isc: float  # Short circuit current (A)

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


class CECDatabase:
    """Interface to CEC module and inverter databases."""

    def __init__(self) -> None:
        self.logger = setup_logger(__name__)
        self.module_db = CEC_MODULES_DB
        self.inverter_db = CEC_INVERTERS_DB

        self.logger.info(
            f"Loaded {len(self.module_db)} modules from hardcoded database"
        )
        self.logger.info(
            f"Loaded {len(self.inverter_db)} inverters from hardcoded database"
        )

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
            area=data["Area"],
            pmax=data["Pmax"],
            vmp=data["Vmp"],
            imp=data["Imp"],
            voc=data["Voc"],
            isc=data["Isc"],
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
        )

    def list_modules(self, search_term: Optional[str] = None) -> list[str]:
        """List available module names, optionally filtered by search term."""
        all_modules = list(self.module_db.keys())

        if search_term:
            search_lower = search_term.lower()
            return [m for m in all_modules if search_lower in m.lower()]

        return all_modules

    def list_inverters(self, search_term: Optional[str] = None) -> list[str]:
        """List available inverter names, optionally filtered by search term."""
        all_inverters = list(self.inverter_db.keys())

        if search_term:
            search_lower = search_term.lower()
            return [i for i in all_inverters if search_lower in i.lower()]

        return all_inverters
