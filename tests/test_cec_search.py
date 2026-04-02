"""Tests for tokenized search and numeric filters in CEC database.

Validates the multi-token AND-matching search logic and STC/Paco filters
added in M20 Issue 4.
"""

import pytest

from src.pysam_integration.cec_database import CECDatabase


@pytest.fixture(scope="module")
def db() -> CECDatabase:
    """Shared CECDatabase instance — loaded once for all tests in this module."""
    return CECDatabase()


# ---------------------------------------------------------------------------
# Tokenized module search
# ---------------------------------------------------------------------------


class TestTokenizedModuleSearch:
    """Multi-token search: all tokens must appear as substrings in module name."""

    def test_multi_token_trina_solar_550(self, db: CECDatabase) -> None:
        """'Trina Solar 550' matches modules with all three tokens."""
        results = db.list_modules("Trina Solar 550")
        assert len(results) > 0, "Expected Trina Solar 550W modules"
        for name in results:
            name_lower = name.lower()
            assert "trina" in name_lower
            assert "solar" in name_lower
            assert "550" in name_lower

    def test_multi_token_csi_solar_cs6w(self, db: CECDatabase) -> None:
        """'CSI Solar CS6W' matches CSI Solar modules with CS6W model prefix."""
        results = db.list_modules("CSI Solar CS6W")
        assert len(results) > 0, "Expected CSI Solar CS6W modules"
        for name in results:
            name_lower = name.lower()
            assert "csi" in name_lower
            assert "solar" in name_lower
            assert "cs6w" in name_lower

    def test_multi_token_csi_solar_cs6w_550ms(self, db: CECDatabase) -> None:
        """'CSI Solar CS6W-550MS' matches despite 'Co. Ltd.' gap in CEC name."""
        results = db.list_modules("CSI Solar CS6W-550MS")
        assert len(results) > 0, "Expected CSI Solar CS6W-550MS modules"
        for name in results:
            assert "CS6W-550MS" in name

    def test_single_token_backward_compat(self, db: CECDatabase) -> None:
        """Single-token search 'Trina' returns same results as old behavior."""
        results = db.list_modules("Trina")
        assert len(results) > 0
        for name in results:
            assert "trina" in name.lower()

    def test_single_token_csi_solar(self, db: CECDatabase) -> None:
        """'CSI Solar' still works as a two-token search."""
        results = db.list_modules("CSI Solar")
        assert len(results) > 100
        for name in results:
            assert "csi" in name.lower()
            assert "solar" in name.lower()

    def test_case_insensitive(self, db: CECDatabase) -> None:
        """Search is case-insensitive for all tokens."""
        results_lower = db.list_modules("trina solar")
        results_upper = db.list_modules("TRINA SOLAR")
        results_mixed = db.list_modules("Trina Solar")
        assert results_lower == results_upper == results_mixed

    def test_no_match_returns_empty(self, db: CECDatabase) -> None:
        """Search with impossible tokens returns empty list."""
        results = db.list_modules("xyznonexistent zzz999")
        assert results == []

    def test_empty_search_returns_all(self, db: CECDatabase) -> None:
        """Empty search returns all modules."""
        results = db.list_modules("")
        assert len(results) > 20000

    def test_none_search_returns_all(self, db: CECDatabase) -> None:
        """None search returns all modules."""
        results = db.list_modules(None)
        assert len(results) > 20000


# ---------------------------------------------------------------------------
# Tokenized inverter search
# ---------------------------------------------------------------------------


class TestTokenizedInverterSearch:
    """Multi-token search on inverters."""

    def test_multi_token_sungrow_250(self, db: CECDatabase) -> None:
        """'Sungrow 250' matches Sungrow inverters with '250' in name."""
        results = db.list_inverters("Sungrow 250")
        assert len(results) > 0, "Expected Sungrow 250 inverters"
        for name in results:
            name_lower = name.lower()
            assert "sungrow" in name_lower
            assert "250" in name_lower

    def test_multi_token_sma_america_sunny(self, db: CECDatabase) -> None:
        """'SMA America' matches SMA inverters."""
        results = db.list_inverters("SMA America")
        assert len(results) > 0
        for name in results:
            name_lower = name.lower()
            assert "sma" in name_lower
            assert "america" in name_lower

    def test_single_token_sungrow(self, db: CECDatabase) -> None:
        """Single-token 'Sungrow' returns all Sungrow inverters."""
        results = db.list_inverters("Sungrow")
        assert len(results) > 0
        for name in results:
            assert "sungrow" in name.lower()

    def test_no_match_returns_empty(self, db: CECDatabase) -> None:
        """No-match search returns empty."""
        results = db.list_inverters("xyznonexistent zzz999")
        assert results == []


# ---------------------------------------------------------------------------
# Numeric STC filters on modules
# ---------------------------------------------------------------------------


class TestModuleSTCFilters:
    """Test min_stc and max_stc numeric filters."""

    def test_min_stc_only(self, db: CECDatabase) -> None:
        """min_stc=500 filters to modules >= 500W STC."""
        results = db.list_modules(min_stc=500)
        assert len(results) > 0
        for name in results:
            assert db.module_db[name]["STC"] >= 500

    def test_max_stc_only(self, db: CECDatabase) -> None:
        """max_stc=100 filters to small modules <= 100W."""
        results = db.list_modules(max_stc=100)
        assert len(results) > 0
        for name in results:
            assert db.module_db[name]["STC"] <= 100

    def test_stc_range(self, db: CECDatabase) -> None:
        """min_stc=540 & max_stc=560 brackets 550W modules."""
        results = db.list_modules(min_stc=540, max_stc=560)
        assert len(results) > 0
        for name in results:
            stc = db.module_db[name]["STC"]
            assert 540 <= stc <= 560

    def test_combined_search_and_stc(self, db: CECDatabase) -> None:
        """'Trina' + min_stc=540 + max_stc=560 finds Trina ~550W modules."""
        results = db.list_modules("Trina", min_stc=540, max_stc=560)
        assert len(results) > 0
        for name in results:
            assert "trina" in name.lower()
            stc = db.module_db[name]["STC"]
            assert 540 <= stc <= 560

    def test_stc_range_no_match(self, db: CECDatabase) -> None:
        """Impossible STC range returns empty."""
        results = db.list_modules(min_stc=99999, max_stc=100000)
        assert results == []


# ---------------------------------------------------------------------------
# Numeric Paco filters on inverters
# ---------------------------------------------------------------------------


class TestInverterPacoFilters:
    """Test min_paco and max_paco numeric filters."""

    def test_min_paco_only(self, db: CECDatabase) -> None:
        """min_paco=200000 filters to inverters >= 200kW."""
        results = db.list_inverters(min_paco=200000)
        assert len(results) > 0
        for name in results:
            assert db.inverter_db[name]["Paco"] >= 200000

    def test_max_paco_only(self, db: CECDatabase) -> None:
        """max_paco=5000 filters to small inverters <= 5kW."""
        results = db.list_inverters(max_paco=5000)
        assert len(results) > 0
        for name in results:
            assert db.inverter_db[name]["Paco"] <= 5000

    def test_paco_range(self, db: CECDatabase) -> None:
        """min_paco=200000 & max_paco=300000 brackets 200-300kW inverters."""
        results = db.list_inverters(min_paco=200000, max_paco=300000)
        assert len(results) > 0
        for name in results:
            paco = db.inverter_db[name]["Paco"]
            assert 200000 <= paco <= 300000

    def test_combined_search_and_paco(self, db: CECDatabase) -> None:
        """'Sungrow' + min_paco=200000 finds large Sungrow inverters."""
        results = db.list_inverters("Sungrow", min_paco=200000)
        assert len(results) > 0
        for name in results:
            assert "sungrow" in name.lower()
            assert db.inverter_db[name]["Paco"] >= 200000

    def test_paco_range_no_match(self, db: CECDatabase) -> None:
        """Impossible Paco range returns empty."""
        results = db.list_inverters(min_paco=999999999)
        assert results == []
