"""Tests for CEC database integration with real CSV data."""

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
        """Test that databases load successfully from CSV files."""
        assert len(db.module_db) > 20000
        assert len(db.inverter_db) > 2000

    def test_list_modules(self, db: CECDatabase) -> None:
        """Test listing modules."""
        all_modules = db.list_modules()
        assert len(all_modules) > 20000
        assert "CSI Solar Co. Ltd. CS3U-355P" in all_modules

        # Test search filtering
        csi_modules = db.list_modules("CSI Solar")
        assert len(csi_modules) > 100
        assert all("CSI Solar" in m for m in csi_modules)

    def test_list_inverters(self, db: CECDatabase) -> None:
        """Test listing inverters."""
        all_inverters = db.list_inverters()
        assert len(all_inverters) > 2000
        assert "SMA America: SB3.0-1SP-US-40 [240V]" in all_inverters

        # Test search filtering
        sma_inverters = db.list_inverters("SMA")
        assert len(sma_inverters) > 0
        assert all("SMA" in i for i in sma_inverters)

    def test_get_module_params(self, db: CECDatabase) -> None:
        """Test retrieving module parameters with CEC five-parameter model fields."""
        params = db.get_module_params("CSI Solar Co. Ltd. CS3U-355P")

        assert params.name == "CSI Solar Co. Ltd. CS3U-355P"
        assert params.pmax == pytest.approx(355.388, rel=0.01)
        assert params.vmp == pytest.approx(39.4, rel=0.01)
        assert params.area == pytest.approx(1.92, rel=0.01)
        assert params.n_s == 72
        assert params.alpha_sc == pytest.approx(0.00480459, rel=0.01)
        assert params.beta_oc == pytest.approx(-0.136843, rel=0.01)
        assert params.t_noct == pytest.approx(44.6, rel=0.01)
        assert params.gamma_pmp == pytest.approx(-0.3811, rel=0.01)
        assert params.technology == "Multi-c-Si"
        assert params.bifacial == 0
        assert params.efficiency > 0

    def test_get_inverter_params(self, db: CECDatabase) -> None:
        """Test retrieving inverter parameters."""
        params = db.get_inverter_params("SMA America: SB3.0-1SP-US-40 [240V]")

        assert params.name == "SMA America: SB3.0-1SP-US-40 [240V]"
        assert params.paco == pytest.approx(3000.0, rel=0.01)
        assert params.vdcmax == pytest.approx(480.0, rel=0.01)
        assert params.mppt_low == pytest.approx(155.0, rel=0.01)
        assert params.mppt_high == pytest.approx(480.0, rel=0.01)
        assert params.vac == pytest.approx(240.0, rel=0.01)
        assert params.idcmax > 0

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
        params = db.get_module_params("CSI Solar Co. Ltd. CS3U-355P")

        # Efficiency = Pmax / (Area * 1000)
        expected_eff = params.pmax / (params.area * 1000)
        assert abs(params.efficiency - expected_eff) < 0.001


def test_generate_database_listing() -> None:
    """Generate listing of available equipment for documentation."""
    db = CECDatabase()

    output_path = Path("outputs/test_results/cec_database_listing.txt")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write("CEC DATABASE CONTENTS (CSV)\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Total modules: {len(db.module_db)}\n")
        f.write(f"Total inverters: {len(db.inverter_db)}\n\n")

        # Sample modules (first 10)
        f.write("SAMPLE MODULES (first 10):\n")
        f.write("-" * 80 + "\n")
        for module in db.list_modules()[:10]:
            params = db.get_module_params(module)
            f.write(f"{module}\n")
            f.write(
                f"  Pmax: {params.pmax:.1f} W, Area: {params.area:.2f} m², "
                f"Eff: {params.efficiency:.3f}\n"
            )

        f.write("\n\n")

        # Sample inverters (first 10)
        f.write("SAMPLE INVERTERS (first 10):\n")
        f.write("-" * 80 + "\n")
        for inverter in db.list_inverters()[:10]:
            params = db.get_inverter_params(inverter)
            f.write(f"{inverter}\n")
            f.write(
                f"  Paco: {params.paco / 1000:.1f} kW, "
                f"MPPT: {params.mppt_low}-{params.mppt_high} V\n"
            )

    print(f"\nGenerated: {output_path}")
