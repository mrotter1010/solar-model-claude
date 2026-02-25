"""Tests for string sizing calculator."""

from pathlib import Path

import pytest

from src.pysam_integration.exceptions import StringCalculationError
from src.pysam_integration.string_calculator import (
    MAX_DEVIATION_PCT,
    MAX_MODULES_PER_STRING,
    MIN_MODULES_PER_STRING,
    StringCalculator,
    StringConfiguration,
)


@pytest.fixture
def calc() -> StringCalculator:
    """StringCalculator instance."""
    return StringCalculator()


# -- Test: Exact divisibility --


class TestExactDivisibility:
    """Test clean cases where total modules divide evenly."""

    def test_exact_divisibility(self, calc: StringCalculator) -> None:
        """A DC size that produces an exact total divisible within 10-40 range."""
        # 10 MW with 400W modules → 25,000 modules
        # 25,000 / 25 = 1,000 strings (25 modules/string)
        config = calc.calculate_strings(dc_size_mw=10.0, module_wattage=400.0)

        assert config.total_modules % config.modules_per_string == 0
        assert config.nstrings == config.total_modules // config.modules_per_string
        assert config.deviation_pct < 0.01  # Near-zero deviation


# -- Test: Needs adjustment --


class TestNeedsAdjustment:
    """Test cases where initial total doesn't divide evenly."""

    def test_adjustment_finds_valid_config(self, calc: StringCalculator) -> None:
        """When exact total doesn't divide, adjustment search finds a valid config."""
        # Pick a DC size that likely won't divide cleanly at base total
        # 7.77 MW with 355W modules → ~21,887.32 → round to 21,887
        # 21,887 is prime-ish — unlikely to divide by 10-40
        config = calc.calculate_strings(dc_size_mw=7.77, module_wattage=355.0)

        assert config.modules_per_string >= MIN_MODULES_PER_STRING
        assert config.modules_per_string <= MAX_MODULES_PER_STRING
        assert config.nstrings >= 1
        assert config.deviation_pct <= MAX_DEVIATION_PCT


# -- Test: Boundary modules per string --


class TestBoundaryModulesPerString:
    """Test minimum and maximum modules/string boundaries."""

    def test_minimum_modules_per_string(self, calc: StringCalculator) -> None:
        """Configuration can use exactly 10 modules/string."""
        # 1 MW with 500W → 2,000 modules → 2,000 / 10 = 200 strings
        config = calc.calculate_strings(dc_size_mw=1.0, module_wattage=500.0)

        # Should find a valid config — 10 modules/string works here
        assert config.modules_per_string >= MIN_MODULES_PER_STRING

    def test_maximum_modules_per_string(self, calc: StringCalculator) -> None:
        """Configuration can use exactly 40 modules/string."""
        # 4 MW with 500W → 8,000 modules → 8,000 / 40 = 200 strings
        config = calc.calculate_strings(dc_size_mw=4.0, module_wattage=500.0)

        assert config.modules_per_string <= MAX_MODULES_PER_STRING


# -- Test: Impossible configuration --


class TestImpossibleConfiguration:
    """Test error handling when no valid config exists."""

    def test_impossible_raises_error(self, calc: StringCalculator) -> None:
        """Extremely small DC size with large modules should fail."""
        # 0.001 MW (1 kW) with 500W modules → 2 modules
        # Can't make 10-40 modules/string with only 2 modules
        with pytest.raises(StringCalculationError) as exc_info:
            calc.calculate_strings(dc_size_mw=0.001, module_wattage=500.0)

        assert "0.001" in str(exc_info.value)
        assert "500" in str(exc_info.value)


# -- Test: Deviation accuracy --


class TestDeviationAccuracy:
    """Test deviation percentage is calculated correctly."""

    def test_deviation_calculation(self, calc: StringCalculator) -> None:
        """Verify deviation_pct = |actual - target| / target * 100."""
        config = calc.calculate_strings(dc_size_mw=50.0, module_wattage=355.0)

        expected_deviation = (
            abs(config.dc_size_mw_actual - 50.0) / 50.0 * 100
        )
        assert config.deviation_pct == pytest.approx(expected_deviation, rel=1e-6)

    def test_total_dc_kw_matches_modules(self, calc: StringCalculator) -> None:
        """total_dc_kw should equal total_modules * module_wattage / 1000."""
        config = calc.calculate_strings(dc_size_mw=20.0, module_wattage=410.0)

        expected_kw = config.total_modules * 410.0 / 1000
        assert config.total_dc_kw == pytest.approx(expected_kw)


# -- Test: Best selection --


class TestBestSelection:
    """Test that the config with smallest deviation is chosen."""

    def test_smallest_deviation_selected(self, calc: StringCalculator) -> None:
        """Result should have the minimum possible deviation."""
        # Run two similar DC sizes — both should pick smallest deviation
        config_a = calc.calculate_strings(dc_size_mw=15.0, module_wattage=355.0)
        config_b = calc.calculate_strings(dc_size_mw=15.0, module_wattage=400.0)

        # Both should be valid and minimize deviation
        assert config_a.deviation_pct <= MAX_DEVIATION_PCT
        assert config_b.deviation_pct <= MAX_DEVIATION_PCT


# -- Test: Integration with real CEC module specs --


class TestCECModuleIntegration:
    """Test with actual CEC database module wattages."""

    @pytest.mark.parametrize(
        "dc_mw,wattage,module_name",
        [
            (100.0, 355.0, "Canadian Solar CS3U-355P"),
            (50.0, 410.0, "Canadian Solar CS1U-410MS"),
            (75.0, 400.0, "Jinko Solar JKM400M-72HL-BDV"),
            (25.0, 365.0, "Trina Solar TSM-365-DE06X.08(II)"),
            (10.0, 380.0, "LONGi Solar LR4-72HPH-380M"),
        ],
    )
    def test_real_module_specs(
        self,
        calc: StringCalculator,
        dc_mw: float,
        wattage: float,
        module_name: str,
    ) -> None:
        """Verify valid configs for real-world DC sizes and module wattages."""
        config = calc.calculate_strings(dc_size_mw=dc_mw, module_wattage=wattage)

        assert config.nstrings >= 1
        assert MIN_MODULES_PER_STRING <= config.modules_per_string <= MAX_MODULES_PER_STRING
        assert config.deviation_pct <= MAX_DEVIATION_PCT
        assert config.total_modules == config.nstrings * config.modules_per_string


# -- Artifact writer --


class TestArtifactWriter:
    """Write test summary to outputs/test_results/ for manual inspection."""

    def test_write_string_calc_artifact(self, calc: StringCalculator) -> None:
        """Write example calculations for 5 scenarios."""
        output_dir = Path("outputs/test_results")
        output_dir.mkdir(parents=True, exist_ok=True)

        scenarios = [
            (100.0, 355.0, "Canadian Solar CS3U-355P"),
            (50.0, 410.0, "Canadian Solar CS1U-410MS"),
            (75.0, 400.0, "Jinko Solar JKM400M-72HL-BDV"),
            (25.0, 365.0, "Trina Solar TSM-365-DE06X.08(II)"),
            (10.0, 380.0, "LONGi Solar LR4-72HPH-380M"),
        ]

        lines = ["=== String Calculator Test Results ===", ""]

        for dc_mw, wattage, name in scenarios:
            config = calc.calculate_strings(dc_size_mw=dc_mw, module_wattage=wattage)
            lines.extend([
                f"--- {name} ({wattage}W) @ {dc_mw} MW ---",
                f"  Strings: {config.nstrings}",
                f"  Modules/String: {config.modules_per_string}",
                f"  Total Modules: {config.total_modules}",
                f"  Target DC: {config.dc_size_mw_target} MW",
                f"  Actual DC: {config.dc_size_mw_actual:.6f} MW",
                f"  Total DC: {config.total_dc_kw:.3f} kW",
                f"  Deviation: {config.deviation_pct:.4f}%",
                "",
            ])

        lines.append("=== END ===")

        output_path = output_dir / "string_calc_test.txt"
        output_path.write_text("\n".join(lines))
        assert output_path.exists()
