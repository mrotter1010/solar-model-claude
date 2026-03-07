"""Tests for the monthly soiling loss calculator."""

import pytest

from src.climate.soiling_calculator import calculate_monthly_soiling


def test_all_thresholds_hit() -> None:
    """Input spanning all precipitation brackets produces correct soiling losses."""
    monthly_precip = [3.0, 2.5, 1.8, 1.2, 0.7, 0.3, 0.1, 0.2, 0.6, 1.5, 2.1, 2.8]
    expected = [1.0, 1.0, 1.5, 2.0, 2.5, 3.0, 3.0, 3.0, 2.5, 1.5, 1.0, 1.0]

    result = calculate_monthly_soiling(monthly_precip)

    assert result == expected


def test_all_dry_months() -> None:
    """Zero precipitation every month produces maximum soiling (3.0%)."""
    result = calculate_monthly_soiling([0.0] * 12)

    assert result == [3.0] * 12


def test_all_wet_months() -> None:
    """Heavy precipitation every month produces minimum soiling (1.0%)."""
    result = calculate_monthly_soiling([5.0] * 12)

    assert result == [1.0] * 12


def test_boundary_at_0_5_inches() -> None:
    """0.5 inches is >= 0.5 threshold, so soiling is 2.5%."""
    # 0.5 exactly hits the >= 0.5 bracket
    precip = [0.5] + [2.0] * 11
    result = calculate_monthly_soiling(precip)
    assert result[0] == 2.5

    # Just below 0.5 falls to the < 0.5 bracket (3.0%)
    precip_below = [0.49] + [2.0] * 11
    result_below = calculate_monthly_soiling(precip_below)
    assert result_below[0] == 3.0


def test_boundary_at_1_0_inches() -> None:
    """1.0 inches is >= 1.0 threshold, so soiling is 2.0%."""
    precip = [1.0] + [2.0] * 11
    result = calculate_monthly_soiling(precip)
    assert result[0] == 2.0

    # Just below 1.0 falls to the >= 0.5 bracket (2.5%)
    precip_below = [0.99] + [2.0] * 11
    result_below = calculate_monthly_soiling(precip_below)
    assert result_below[0] == 2.5


def test_boundary_at_1_5_inches() -> None:
    """1.5 inches is >= 1.5 threshold, so soiling is 1.5%."""
    precip = [1.5] + [2.0] * 11
    result = calculate_monthly_soiling(precip)
    assert result[0] == 1.5

    # Just below 1.5 falls to the >= 1.0 bracket (2.0%)
    precip_below = [1.49] + [2.0] * 11
    result_below = calculate_monthly_soiling(precip_below)
    assert result_below[0] == 2.0


def test_boundary_at_2_0_inches() -> None:
    """2.0 inches is >= 2.0 threshold, so soiling is 1.0%."""
    precip = [2.0] + [0.0] * 11
    result = calculate_monthly_soiling(precip)
    assert result[0] == 1.0

    # Just below 2.0 falls to the >= 1.5 bracket (1.5%)
    precip_below = [1.99] + [0.0] * 11
    result_below = calculate_monthly_soiling(precip_below)
    assert result_below[0] == 1.5


def test_wrong_length_11_raises_value_error() -> None:
    """Input with 11 elements raises ValueError."""
    with pytest.raises(ValueError, match="Expected 12 monthly values, got 11"):
        calculate_monthly_soiling([1.0] * 11)


def test_wrong_length_13_raises_value_error() -> None:
    """Input with 13 elements raises ValueError."""
    with pytest.raises(ValueError, match="Expected 12 monthly values, got 13"):
        calculate_monthly_soiling([1.0] * 13)


def test_empty_list_raises_value_error() -> None:
    """Empty list raises ValueError."""
    with pytest.raises(ValueError, match="Expected 12 monthly values, got 0"):
        calculate_monthly_soiling([])
