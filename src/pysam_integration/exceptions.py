"""Custom exceptions for PySAM integration."""


class PySAMConfigurationError(Exception):
    """Base exception for PySAM model configuration errors."""

    pass


class ValidationError(PySAMConfigurationError):
    """Raised when a configuration value fails validation."""

    pass


class InverterUndersizedError(PySAMConfigurationError):
    """Raised when inverter AC capacity is too small relative to DC capacity."""

    def __init__(self, dc_ac_ratio: float, max_ratio: float = 2.0) -> None:
        self.dc_ac_ratio = dc_ac_ratio
        self.max_ratio = max_ratio
        super().__init__(
            f"DC/AC ratio {dc_ac_ratio:.2f} exceeds maximum {max_ratio:.1f}. "
            f"Inverter is undersized for the DC array capacity."
        )


class CECDatabaseError(Exception):
    """Base exception for CEC database errors."""

    pass


class ModuleNotFoundError(CECDatabaseError):
    """Raised when module name not found in database."""

    def __init__(self, module_name: str, similar_modules: list[str]) -> None:
        self.module_name = module_name
        self.similar_modules = similar_modules

        similar_str = (
            "\n".join(f"  - {m}" for m in similar_modules[:5])
            if similar_modules
            else "  (none)"
        )
        super().__init__(
            f"Module '{module_name}' not found in CEC database.\n"
            f"Similar modules:\n{similar_str}\n\n"
            f"Available modules: {', '.join(similar_modules[:3]) if similar_modules else 'See logs for full list'}\n"
            f"Please update CSV with exact module name from database."
        )


class InverterNotFoundError(CECDatabaseError):
    """Raised when inverter name not found in database."""

    def __init__(self, inverter_name: str, similar_inverters: list[str]) -> None:
        self.inverter_name = inverter_name
        self.similar_inverters = similar_inverters

        similar_str = (
            "\n".join(f"  - {i}" for i in similar_inverters[:5])
            if similar_inverters
            else "  (none)"
        )
        super().__init__(
            f"Inverter '{inverter_name}' not found in CEC database.\n"
            f"Similar inverters:\n{similar_str}\n\n"
            f"Available inverters: {', '.join(similar_inverters[:3]) if similar_inverters else 'See logs for full list'}\n"
            f"Please update CSV with exact inverter name from database."
        )
