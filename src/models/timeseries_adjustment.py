"""Apply subhourly resolution correction to 8760 hourly generation timeseries.

Adjusts an hourly PySAM generation profile to account for irradiance variability
effects that hourly resolution misses. The correction targets physically
meaningful hours: clipping-prone hours for energy removal, and high-irradiance
hours for energy addition.

Typical adjustments are small (< 2% of annual energy) and preserve the hourly
generation shape while correcting the annual total.
"""

from __future__ import annotations

import numpy as np

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _shave_peaks(
    adjusted: np.ndarray,
    indices: list[int],
    target: float,
) -> float:
    """Remove energy by shaving generation peaks at specified hours.

    Uses a waterfall approach: draws a horizontal cut line starting from the
    highest peak and pushes it down until the total area above the line equals
    the removal target. This produces physically realistic adjustments — the
    highest-clipping hours lose the most energy.

    Args:
        adjusted: Mutable generation array (modified in place).
        indices: Hour indices eligible for shaving.
        target: Total energy (kWh) to remove.

    Returns:
        Total energy actually removed.
    """
    if not indices or target <= 0:
        return 0.0

    # Sort eligible hours by generation descending
    values = np.array([adjusted[i] for i in indices])
    sorted_order = np.argsort(-values)
    sorted_indices = [indices[j] for j in sorted_order]
    sorted_values = values[sorted_order]

    removed = 0.0
    n = len(sorted_values)

    for k in range(n):
        # All hours 0..k are currently at sorted_values[k] level.
        # Next level down is sorted_values[k+1], or 0 if at the bottom.
        next_level = sorted_values[k + 1] if k + 1 < n else 0.0
        width = k + 1
        layer_energy = width * (sorted_values[k] - next_level)

        remaining = target - removed

        if layer_energy >= remaining:
            # Finish within this layer
            trim = remaining / width
            cut_level = sorted_values[k] - trim
            for j in range(width):
                adjusted[sorted_indices[j]] = max(0.0, cut_level)
            removed += remaining
            break
        else:
            # Consume entire layer, move to next
            for j in range(width):
                adjusted[sorted_indices[j]] = next_level
            removed += layer_energy

    return removed


def _add_proportional(
    adjusted: np.ndarray,
    indices: list[int],
    dc_ratios: list[float],
    target: float,
    ac_capacity_kw: float,
) -> float:
    """Add energy proportionally to DC/AC ratio, capped at AC capacity.

    Higher DC/AC ratio hours receive a larger share of the energy boost,
    reflecting the physical mechanism: hours with more DC headroom above
    the inverter limit are where subhourly peaks would have generated
    additional clipped energy.

    Args:
        adjusted: Mutable generation array (modified in place).
        indices: Hour indices eligible for energy addition.
        dc_ratios: DC/AC ratio for each eligible hour (parallel to indices).
        target: Total energy (kWh) to add.
        ac_capacity_kw: Maximum AC output per hour (kW = kWh for hourly).

    Returns:
        Total energy actually added.
    """
    if not indices or target <= 0:
        return 0.0

    total_weight = sum(dc_ratios)
    if total_weight <= 0:
        return 0.0

    added = 0.0
    for i, idx in enumerate(indices):
        share = target * (dc_ratios[i] / total_weight)
        headroom = max(0.0, ac_capacity_kw - adjusted[idx])
        addition = min(share, headroom)
        adjusted[idx] += addition
        added += addition

    return added


def apply_correction(
    hourly_gen_kwh: list[float],
    correction_pct: float,
    dc_hourly_kwh: list[float],
    ac_capacity_kw: float,
) -> list[float]:
    """Apply subhourly resolution correction to an 8760 generation timeseries.

    For positive corrections (hourly overestimates), energy is shaved from
    clipping-prone hours using a waterfall approach. For negative corrections
    (hourly underestimates), energy is added to high-irradiance hours
    proportionally to their DC/AC ratio.

    Args:
        hourly_gen_kwh: 8760 AC generation values in kWh (or kW for hourly).
        correction_pct: Correction from predict_correction(). Positive means
            hourly overestimates (remove energy), negative means hourly
            underestimates (add energy).
        dc_hourly_kwh: 8760 DC generation values in kWh, used to identify
            clipping-prone and high-irradiance hours.
        ac_capacity_kw: Inverter AC capacity in kW.

    Returns:
        Adjusted 8760 generation timeseries as list[float].
    """
    if correction_pct == 0.0:
        return list(hourly_gen_kwh)

    adjusted = np.array(hourly_gen_kwh, dtype=float)
    original_total = adjusted.sum()

    if original_total <= 0:
        logger.warning("No generation to correct (total = 0)")
        return list(hourly_gen_kwh)

    # Compute per-hour DC/AC ratio
    dc_ac_ratio = np.array(dc_hourly_kwh, dtype=float) / ac_capacity_kw

    if correction_pct > 0:
        # --- Positive: hourly overestimates, REMOVE energy ---
        target_removal = original_total * correction_pct / 100.0

        # Identify clipping-prone hours (DC/AC > 0.90)
        clipping_mask = dc_ac_ratio > 0.90
        clipping_indices = list(np.where(clipping_mask)[0])

        removed = _shave_peaks(adjusted, clipping_indices, target_removal)

        # If clipping hours weren't sufficient, spread remainder proportionally
        shortfall = target_removal - removed
        if shortfall > 0.01:
            generating_mask = adjusted > 0
            generating_indices = list(np.where(generating_mask)[0])
            gen_values = adjusted[generating_indices]
            gen_total = gen_values.sum()

            if gen_total > 0:
                scale = max(0.0, 1.0 - shortfall / gen_total)
                adjusted[generating_indices] *= scale
                removed += shortfall

        logger.info(
            f"Subhourly adjustment: removed {removed:.1f} kWh "
            f"({correction_pct:+.3f}%) from {len(clipping_indices)} "
            f"clipping-prone hours"
        )

    else:
        # --- Negative: hourly underestimates, ADD energy ---
        target_addition = original_total * abs(correction_pct) / 100.0

        # Identify high-irradiance hours (DC/AC > 0.70)
        high_irr_mask = dc_ac_ratio > 0.70
        high_irr_indices = list(np.where(high_irr_mask)[0])
        high_irr_ratios = [dc_ac_ratio[i] for i in high_irr_indices]

        # Sort by DC/AC ratio descending
        paired = sorted(
            zip(high_irr_indices, high_irr_ratios),
            key=lambda x: -x[1],
        )
        if paired:
            high_irr_indices, high_irr_ratios = zip(*paired)
            high_irr_indices = list(high_irr_indices)
            high_irr_ratios = list(high_irr_ratios)

        added = _add_proportional(
            adjusted, high_irr_indices, high_irr_ratios,
            target_addition, ac_capacity_kw,
        )

        logger.info(
            f"Subhourly adjustment: added {added:.1f} kWh "
            f"({correction_pct:+.3f}%) across {len(high_irr_indices)} "
            f"high-irradiance hours"
        )

    # Final safety: clamp to [0, ac_capacity_kw]
    np.clip(adjusted, 0.0, ac_capacity_kw, out=adjusted)

    return adjusted.tolist()


def recalculate_summary_metrics(
    adjusted_gen_kwh: list[float],
    dc_capacity_kw: float,
    ac_capacity_kw: float,
) -> dict[str, float]:
    """Recalculate summary metrics from an adjusted generation timeseries.

    Args:
        adjusted_gen_kwh: Adjusted 8760 AC generation values in kWh.
        dc_capacity_kw: System DC nameplate capacity in kW.
        ac_capacity_kw: Inverter AC capacity in kW.

    Returns:
        Dict with keys: total_generation_kwh, net_capacity_factor,
        yield_kwh_per_kwp, performance_ratio.
    """
    gen = np.array(adjusted_gen_kwh, dtype=float)
    total = float(gen.sum())
    hours_in_year = 8760

    net_cf = total / (ac_capacity_kw * hours_in_year) if ac_capacity_kw > 0 else 0.0
    specific_yield = total / dc_capacity_kw if dc_capacity_kw > 0 else 0.0
    # Performance ratio: ratio of actual specific yield to theoretical maximum
    # (equivalent to DC-based capacity factor)
    pr = total / (dc_capacity_kw * hours_in_year) if dc_capacity_kw > 0 else 0.0

    return {
        "total_generation_kwh": round(total, 2),
        "net_capacity_factor": round(net_cf, 6),
        "yield_kwh_per_kwp": round(specific_yield, 2),
        "performance_ratio": round(pr, 6),
    }
