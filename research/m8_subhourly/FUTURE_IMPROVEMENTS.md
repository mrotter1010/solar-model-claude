# Subhourly Resolution Correction — Future Improvements

This document captures improvement paths for the subhourly resolution correction model (Milestone 8). Each section is written with enough context to be picked up cold by a future developer.

## Current Model Specs

| Attribute | Value |
|-----------|-------|
| **Model type** | `GradientBoostingRegressor` (scikit-learn) |
| **Version** | v1 |
| **Training date** | 2026-03-10 |
| **Training data** | 3,240 rows (6,480 paired PySAM simulations: 45 CONUS sites x 72 configs x 2 resolutions) |
| **Target variable** | `resolution_correction_pct` — percentage difference between 5-min and 60-min PySAM annual energy |
| **Artifact location** | `src/models/artifacts/subhourly_correction_v1.joblib` + `_metadata.json` |

**Feature list (14 features):**

| Feature | Source | Description |
|---------|--------|-------------|
| `dcac_ratio` | Input CSV | DC/AC ratio of the system (1.10-1.70 in training) |
| `gcr` | Input CSV | Ground coverage ratio |
| `racking` | Input CSV | Binary: 1=tracker, 0=fixed |
| `latitude` | Input CSV | Site latitude (25-48 in training) |
| `longitude` | Input CSV | Site longitude (-125 to -70 in training) |
| `cf_60min` | PySAM output | Capacity factor from hourly simulation (%) |
| `annual_ghi` | Weather file | Annual GHI (kWh/m2) |
| `mean_kt` | Weather file | Mean clearness index (Kt = GHI / extraterrestrial GHI) |
| `std_kt` | Weather file | Std deviation of clearness index |
| `ghi_cv` | Weather file | Coefficient of variation of daytime GHI |
| `mean_dni` | Weather file | Mean daytime DNI (W/m2) |
| `pct_clear_hours` | Weather file | % of daytime hours with Kt > 0.7 |
| `climate_cloudy` | Derived | One-hot: 1 if pct_clear_hours < 30% |
| `climate_variable` | Derived | One-hot: 1 if pct_clear_hours 30-45% |

**LOSO-CV metrics (leave-one-site-out cross-validation, 45 folds):**

| Metric | Value |
|--------|-------|
| RMSE | 0.180% |
| MAE | 0.139% |
| R2 (global, pooled predictions) | 0.848 |
| R2 (mean across folds) | 0.645 |

**Correction range (from training data):**

| Statistic | Value |
|-----------|-------|
| Min | -1.67% (tracker, clear desert, low DC/AC) |
| Max | +0.61% (fixed, cloudy PNW, high DC/AC) |
| Mean | -0.42% |
| Std | 0.46% |

**Clamp rationale:** The raw model output is clamped to >= 0 (loss-only) before application. Negative raw predictions (suggesting hourly resolution underestimates energy) are artifacts of satellite data temporal smoothing — 5-min NSRDB satellite data cannot capture the true irradiance peaks that 1-min ground stations measure. Per DNV's Hourly Modeling Correction (HMC) standard, subhourly resolution effects are treated as losses only. This clamp should be revisited when the model is retrained on 1-min ground station data (see Improvement 1 below).

---

## Improvement 1: 1-Minute Ground Station Training Data

**Priority: High | Effort: High | Impact: High**

**Problem:** The current model is trained on paired 5-min vs 60-min NSRDB satellite data. NSRDB 5-min data already smooths irradiance variability relative to ground truth — satellite pixels average over ~4 km and have inherent temporal interpolation. This means our model captures only a fraction of the true subhourly resolution effect. Published corrections using 1-min ground data are 1-4% (compared to our 0-0.6% clamped range).

**Approach:** Train on paired 1-min vs 60-min PySAM runs using public ground station networks:
- **SURFRAD** (7 stations, NOAA): 1-min GHI, DNI, DHI. Bonneville, Desert Rock, Fort Peck, Goodwin Creek, Penn State, Sioux Falls, Table Mountain.
- **SOLRAD** (7 stations, NOAA): 1-min GHI, DNI, DHI. Primarily eastern US.
- **MIDC** (~15 stations, NREL): 1-min irradiance at various NREL facilities and partner sites.

Total: ~29 stations with public 1-min irradiance data. PySAM can accept 1-min weather input directly.

**Expected outcome:** Correction magnitudes increase to match the published 1-4% range. The clamped-to-zero policy could potentially be relaxed if ground-truth data consistently shows losses across all site types (including clear desert trackers that currently show "gains" with satellite data).

**Key challenge:** Ground stations have limited geographic diversity — mostly research sites, not representative of all US climates. May need to combine with satellite data: use ground stations for calibration and satellite for geographic coverage.

**Reference:** Anderson & Perry (2020), "A Framework for Subhourly Corrections to PV Performance Models," NREL/CP-5K00-76021. This is the foundational paper for subhourly correction methodology.

## Improvement 2: Multi-Year Training Data

**Priority: Medium | Effort: Low | Impact: Moderate**

**Problem:** The current model is trained on 2022 data only. A single year captures one realization of weather patterns — an unusually clear or cloudy year biases the model. Inter-annual variability in cloud patterns, atmospheric aerosols, and weather systems is not represented.

**Approach:** Run the same 45-site x 72-config simulation matrix for years 2019-2023 (NSRDB 5-min data available from 2018 onward). This 5x increase in training data requires:
1. Modify `batch_runner.py` to accept a `--years` parameter
2. Run 5-min and 60-min PySAM for each year (5 x 6,480 = 32,400 simulations)
3. Add `year` as a feature or train across all years without it (weather features already capture annual climate)

**Expected outcome:** More robust model, especially for sites where 2022 was climatologically unusual. The mean fold R2 (currently 0.645) should improve as the model sees more weather pattern variety per site.

**Technical note:** Each PySAM run takes ~2-3 seconds. 32,400 runs = ~18-27 hours of computation (parallelizable). Storage: ~3.5 GB of raw results at ~100 KB per run.

## Improvement 3: Additional Training Sites

**Priority: Medium | Effort: Low-Medium | Impact: Moderate**

**Problem:** The 5 worst-performing LOSO-CV sites (Tallahassee, Denver, Amarillo, Daggett, Fresno) are climatically unique — their local weather patterns are poorly interpolated from other training sites. Adding sites in similar climates would improve generalization.

**Approach:** Add 20-30 NSRDB grid points in underrepresented climate transition zones:
- **Gulf Coast** (2-4 sites): Mobile AL, Pensacola FL, Biloxi MS — humid subtropical with high variability, similar to Tallahassee
- **Front Range** (2-3 sites): Colorado Springs, Boulder, Pueblo — orographic cloud effects similar to Denver
- **Great Lakes shoreline** (3-4 sites): Buffalo, Cleveland, Milwaukee — lake-effect cloud patterns
- **Central Valley** (2-3 sites): Sacramento, Bakersfield, Modesto — winter fog/inversion similar to Fresno
- **High Plains** (2-3 sites): Lubbock, Dodge City, North Platte — wind/dust similar to Amarillo
- **Desert outliers** (2-3 sites): Tucson, Las Vegas, El Paso — extreme clear-sky sites similar to Daggett

The `verify_site_grid.py` script in `research/m8_subhourly/` validates NSRDB 5-min data availability for candidate grid points.

**Expected outcome:** Improved LOSO R2 for worst-case folds. Current mean fold R2 = 0.645 is dragged down by these sites.

## Improvement 4: Per-Interval Prediction

**Priority: High | Effort: Very High | Impact: Very High**

**Problem:** The current model predicts one annual correction percentage per site-config combination. This ignores temporal variation — subhourly clipping losses concentrate in summer midday hours when irradiance is highest and most variable. An annual-average correction cannot distinguish between a site that clips heavily in July and one that clips moderately year-round.

**Approach:** Predict corrections at 30-minute or hourly intervals using time-varying features, following the NREL approach:
- **Features per interval:** POA irradiance, clearsky POA, cell temperature, POA rate of change (velocity), DC/AC ratio, ambient temperature
- **Target per interval:** Energy difference between 5-min and 60-min PySAM for that interval
- **Architecture:** Separate models for clipping loss (positive correction) and variability gain (negative correction), applied to each simulation interval

This is a fundamentally different architecture from the current annual-average model. It requires:
1. Generating per-interval training data (not just annual deltas)
2. New feature engineering pipeline for temporal features
3. New timeseries adjustment code to apply per-interval corrections
4. Significantly larger training dataset (~8760 x current size)

**Expected outcome:** Much more accurate corrections, especially for sites with seasonal clipping patterns. Can capture diurnal variation in correction magnitude.

**Reference:** Anderson & Perry (2020), Section III.C: "The correction is applied at each simulation interval, not as an annual scalar."

## Improvement 5: Ensemble Model

**Priority: Low | Effort: Low | Impact: Low-Moderate**

**Problem:** The current single Gradient Boosting model may have systematic biases for certain site/config combinations. In our model evaluation, linear regression and gradient boosting showed complementary strengths — linear regression captured the global trend while gradient boosting fit local nonlinearities.

**Approach:** Implement a stacking or blending ensemble:
- **Option A: 50/50 blend** — Average predictions from Gradient Boosting and Random Forest (the approach used by NREL in Anderson & Perry 2020)
- **Option B: Stacked generalization** — Train a meta-learner (e.g., ridge regression) on out-of-fold predictions from GB, RF, and linear regression
- **Option C: Feature-weighted blend** — Weight model contributions by climate type (GB dominates for variable climates, linear dominates for clear/cloudy extremes)

**Expected outcome:** Small improvement in worst-case predictions. The NREL paper found RF + XGBoost ensemble outperformed either model alone, but the improvement was modest (~5% reduction in RMSE).

**Technical note:** Ensemble adds complexity to the artifact loading and prediction pipeline. Current single-model architecture is deliberately simple. Only pursue if single-model improvements plateau.

## Improvement 6: Spatial Adjustment Factor

**Priority: Medium | Effort: High | Impact: Moderate**

**Problem:** The current model predicts corrections for a point location. Real utility-scale solar plants span 100-2000+ acres. Irradiance variability is spatially correlated — a cloud shadow doesn't cover the entire plant simultaneously. This spatial averaging reduces the effective subhourly variability experienced by the plant, meaning our point-source correction overpredicts losses for large plants.

**Approach:** Implement the DNV spatial adjustment using the Wavelet Variability Model (WVM):
1. Estimate plant footprint from DC capacity and GCR (approximate area in km2)
2. Compute the spatial smoothing factor using WVM for the site's climate type and plant size
3. Multiply the point-source correction by the spatial smoothing factor (always <= 1.0)

The WVM is implemented in `pvlib-python` as `pvlib.scaling.wvm()`. It requires:
- Plant geometry (area, aspect ratio)
- Cloud speed climatology for the site (available from reanalysis data)

**Expected outcome:** Reduced correction magnitudes for large plants (500+ MW), more accurate corrections for small plants (< 50 MW). Currently all plants are treated as point sources regardless of size.

**Reference:** Hayes et al. (2022), "Spatial Adjustment of Subhourly Solar Variability for PV Performance Modeling," IEEE PVSC. Extends Anderson & Perry with DNV's spatial model.

## Improvement 7: LOSO-CV Worst Sites

**Priority: Reference | Effort: N/A | Impact: N/A**

The 5 worst-performing sites by LOSO-CV R2, documented here for future training data targeting:

| Site | R2 | Climate | Challenge |
|------|----|---------|-----------|
| **Tallahassee, FL** | Low | Gulf Coast humid subtropical | High convective variability, sea breeze thunderstorms, transition zone between humid SE and Gulf |
| **Denver, CO** | Low | Continental/orographic | Complex terrain, orographic cloud formation, hail/severe storm frequency, rapid weather changes |
| **Amarillo, TX** | Low | High Plains semiarid | Dust storms, extreme wind variability, isolated thunderstorm cells, continental exposure |
| **Daggett, CA** | Low | Extreme Mojave desert | Extreme clear-sky outlier with occasional dust events, very low variability baseline makes small errors proportionally large |
| **Fresno, CA** | Low | Central Valley Mediterranean | Winter Tule fog/inversion trapping, very high summer GHI, extreme seasonal contrast |

**Common thread:** Each site has a climatically unique feature (orography, fog, dust, sea breeze, extreme aridity) that is not well-represented by other training sites. Adding 2-3 nearby sites for each (see Improvement 3) would give the model more examples of each microclimate.

## Improvement 8: Solcast Subhourly Data

**Priority: Low (future milestone dependent) | Effort: Medium | Impact: Uncertain**

**Problem:** The current correction model is trained on NSRDB satellite data. When Solcast integration is fully built out (beyond the current M7 TMY file ingest), Solcast may offer subhourly resolution data (5-min or 15-min) for specific sites. This could enable site-specific corrections instead of the ML model's generalized prediction.

**Approach:** Two possible paths:
- **Path A: Solcast as alternative training data** — If Solcast provides 5-min irradiance, use it to train a Solcast-specific correction model (separate from the NSRDB-trained model). Solcast satellite data may have different temporal smoothing characteristics than NSRDB.
- **Path B: Solcast as runtime correction** — If Solcast provides subhourly data for the project site, run PySAM at both hourly and subhourly resolution using Solcast data, and compute the site-specific correction directly instead of using the ML model. This is the "gold standard" approach but requires per-site subhourly data access.

**Dependency:** Requires Solcast API integration beyond the current TMY file ingest. The current M7 implementation reads pre-downloaded Solcast TMY files — it does not call the Solcast API directly.

**Investigation needed:** Does Solcast offer 5-min historical data through their API? At what cost? For how many years? These questions need to be answered before scoping this improvement.
