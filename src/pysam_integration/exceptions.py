"""Custom exceptions for PySAM integration."""


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
