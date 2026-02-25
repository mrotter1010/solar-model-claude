"""String sizing calculator for PySAM array configuration."""

from dataclasses import dataclass

from src.pysam_integration.exceptions import StringCalculationError
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

MIN_MODULES_PER_STRING = 10
MAX_MODULES_PER_STRING = 40
MAX_DEVIATION_PCT = 2.0


@dataclass
class StringConfiguration:
    """Result of string sizing calculation."""

    nstrings: int
    modules_per_string: int
    total_modules: int
    total_dc_kw: float
    dc_size_mw_target: float
    dc_size_mw_actual: float
    deviation_pct: float


class StringCalculator:
    """Calculates string sizing from DC capacity and module wattage."""

    def calculate_strings(
        self, dc_size_mw: float, module_wattage: float
    ) -> StringConfiguration:
        """Calculate optimal string configuration.

        Args:
            dc_size_mw: Target DC system size in MW.
            module_wattage: Module power rating in watts (Pmax).

        Returns:
            StringConfiguration with the best valid configuration.

        Raises:
            StringCalculationError: If no valid configuration found.
        """
        total_modules = self._calculate_total_modules(dc_size_mw, module_wattage)

        config = self._find_best_configuration(dc_size_mw, module_wattage)
        if config is None:
            raise StringCalculationError(dc_size_mw, module_wattage, total_modules)

        logger.info(
            f"String config: {config.nstrings} strings × "
            f"{config.modules_per_string} modules/string = "
            f"{config.total_modules} modules, "
            f"actual DC={config.dc_size_mw_actual:.4f} MW "
            f"(deviation={config.deviation_pct:.3f}%)"
        )

        if config.deviation_pct > 1.0:
            logger.warning(
                f"String configuration deviation {config.deviation_pct:.3f}% "
                f"exceeds 1% — review sizing for {dc_size_mw} MW system."
            )

        return config

    def _calculate_total_modules(
        self, dc_size_mw: float, module_wattage: float
    ) -> int:
        """Calculate total modules needed from DC size and module wattage.

        Args:
            dc_size_mw: Target DC size in MW.
            module_wattage: Module Pmax in watts.

        Returns:
            Rounded total module count.
        """
        dc_kw = dc_size_mw * 1000
        return round(dc_kw * 1000 / module_wattage)

    def _try_configuration(
        self,
        total_modules: int,
        modules_per_string: int,
        module_wattage: float,
        dc_size_mw_target: float,
    ) -> StringConfiguration | None:
        """Try a specific modules-per-string value.

        Args:
            total_modules: Total module count to test.
            modules_per_string: Candidate modules per string.
            module_wattage: Module Pmax in watts.
            dc_size_mw_target: Target DC size in MW.

        Returns:
            StringConfiguration if evenly divisible, None otherwise.
        """
        if total_modules % modules_per_string != 0:
            return None

        nstrings = total_modules // modules_per_string
        if nstrings < 1:
            return None

        total_dc_kw = total_modules * module_wattage / 1000
        dc_size_mw_actual = total_dc_kw / 1000
        deviation_pct = (
            abs(dc_size_mw_actual - dc_size_mw_target) / dc_size_mw_target * 100
        )

        return StringConfiguration(
            nstrings=nstrings,
            modules_per_string=modules_per_string,
            total_modules=total_modules,
            total_dc_kw=total_dc_kw,
            dc_size_mw_target=dc_size_mw_target,
            dc_size_mw_actual=dc_size_mw_actual,
            deviation_pct=deviation_pct,
        )

    def _find_best_configuration(
        self, dc_size_mw: float, module_wattage: float
    ) -> StringConfiguration | None:
        """Find the best string configuration within constraints.

        Tries all modules-per-string values from 10-40. If the initial total
        module count doesn't divide evenly, adjusts ±5% and picks the config
        with the smallest deviation from target DC size.

        Args:
            dc_size_mw: Target DC size in MW.
            module_wattage: Module Pmax in watts.

        Returns:
            Best StringConfiguration, or None if no valid config found.
        """
        base_total = self._calculate_total_modules(dc_size_mw, module_wattage)
        candidates: list[StringConfiguration] = []

        # Try the base total first
        for mps in range(MIN_MODULES_PER_STRING, MAX_MODULES_PER_STRING + 1):
            config = self._try_configuration(
                base_total, mps, module_wattage, dc_size_mw
            )
            if config is not None and config.deviation_pct <= MAX_DEVIATION_PCT:
                candidates.append(config)

        # If no exact match, search adjusted totals within ±5%
        if not candidates:
            lower = int(base_total * 0.95)
            upper = int(base_total * 1.05) + 1
            for total in range(lower, upper):
                if total == base_total:
                    continue
                for mps in range(MIN_MODULES_PER_STRING, MAX_MODULES_PER_STRING + 1):
                    config = self._try_configuration(
                        total, mps, module_wattage, dc_size_mw
                    )
                    if config is not None and config.deviation_pct <= MAX_DEVIATION_PCT:
                        candidates.append(config)

        if not candidates:
            return None

        # Pick config with smallest deviation
        return min(candidates, key=lambda c: c.deviation_pct)
