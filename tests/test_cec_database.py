"""Tests for CEC database integration."""

import pytest
from pathlib import Path

from src.pysam_integration.cec_database import CECDatabase, CECModuleParams, CECInverterParams
from src.pysam_integration.exceptions import ModuleNotFoundError, InverterNotFoundError


class TestCECDatabase:
    """Test CEC database integration."""

    @pytest.fixture
    def db(self) -> CECDatabase:
        return CECDatabase()

    def test_database_loads(self, db: CECDatabase) -> None:
        """Test that databases load successfully."""
        assert len(db.module_db) == 20
        assert len(db.inverter_db) == 20

    def test_list_modules(self, db: CECDatabase) -> None:
        """Test listing modules."""
        all_modules = db.list_modules()
        assert len(all_modules) == 20
        assert "Canadian Solar CS3U-355P" in all_modules

        # Test search filtering
        canadian_modules = db.list_modules("Canadian")
        assert len(canadian_modules) == 2
        assert all("Canadian" in m for m in canadian_modules)

    def test_list_inverters(self, db: CECDatabase) -> None:
        """Test listing inverters."""
        all_inverters = db.list_inverters()
        assert len(all_inverters) == 20
        assert "SMA America: SB5.0-1SP-US-40 240V" in all_inverters

        # Test search filtering
        sma_inverters = db.list_inverters("SMA")
        assert len(sma_inverters) > 0
        assert all("SMA" in i for i in sma_inverters)

    def test_get_module_params(self, db: CECDatabase) -> None:
        """Test retrieving module parameters."""
        params = db.get_module_params("Canadian Solar CS3U-355P")

        assert params.name == "Canadian Solar CS3U-355P"
        assert params.pmax == 355.0
        assert params.vmp == 39.1
        assert params.area == 1.94
        assert params.efficiency > 0

    def test_get_inverter_params(self, db: CECDatabase) -> None:
        """Test retrieving inverter parameters."""
        params = db.get_inverter_params("SMA America: SB5.0-1SP-US-40 240V")

        assert params.name == "SMA America: SB5.0-1SP-US-40 240V"
        assert params.paco == 5000.0
        assert params.vdcmax == 600.0
        assert params.mppt_low == 80.0
        assert params.mppt_high == 600.0

    def test_module_not_found(self, db: CECDatabase) -> None:
        """Test error handling for missing module with fuzzy suggestions."""
        with pytest.raises(ModuleNotFoundError) as exc_info:
            db.get_module_params("NonExistent Module XYZ-9999")

        assert "NonExistent Module XYZ-9999" in str(exc_info.value)

    def test_inverter_not_found(self, db: CECDatabase) -> None:
        """Test error handling for missing inverter with fuzzy suggestions."""
        with pytest.raises(InverterNotFoundError) as exc_info:
            db.get_inverter_params("NonExistent Inverter ABC-9999")

        assert "NonExistent Inverter ABC-9999" in str(exc_info.value)

    def test_module_efficiency_calculation(self, db: CECDatabase) -> None:
        """Test module efficiency is calculated correctly."""
        params = db.get_module_params("Canadian Solar CS3U-355P")

        # Efficiency = Pmax / (Area * 1000)
        expected_eff = 355.0 / (1.94 * 1000)
        assert abs(params.efficiency - expected_eff) < 0.001


def test_generate_database_listing() -> None:
    """Generate listing of available equipment for documentation."""
    db = CECDatabase()

    output_path = Path("outputs/test_results/cec_database_listing.txt")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write("CEC DATABASE CONTENTS (HARDCODED SAMPLE)\n")
        f.write("=" * 80 + "\n\n")

        # All modules
        f.write("AVAILABLE MODULES (20):\n")
        f.write("-" * 80 + "\n")
        for module in db.list_modules():
            params = db.get_module_params(module)
            f.write(f"{module}\n")
            f.write(
                f"  Pmax: {params.pmax} W, Area: {params.area} m², "
                f"Eff: {params.efficiency:.3f}\n"
            )

        f.write("\n\n")

        # All inverters
        f.write("AVAILABLE INVERTERS (20):\n")
        f.write("-" * 80 + "\n")
        for inverter in db.list_inverters():
            params = db.get_inverter_params(inverter)
            f.write(f"{inverter}\n")
            f.write(
                f"  Paco: {params.paco / 1000:.1f} kW, "
                f"MPPT: {params.mppt_low}-{params.mppt_high} V\n"
            )

    print(f"\nGenerated: {output_path}")
