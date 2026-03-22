"""SQLAlchemy ORM models for the solar model database."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all ORM models."""


# ---------------------------------------------------------------------------
# customers
# ---------------------------------------------------------------------------

class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    sites: Mapped[list["Site"]] = relationship(back_populates="customer")

    def __repr__(self) -> str:
        return f"<Customer(id={self.id}, name={self.name!r})>"


# ---------------------------------------------------------------------------
# sites
# ---------------------------------------------------------------------------

class Site(Base):
    __tablename__ = "sites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    customer_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("customers.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    customer: Mapped["Customer"] = relationship(back_populates="sites")
    runs: Mapped[list["Run"]] = relationship(back_populates="site")

    def __repr__(self) -> str:
        return f"<Site(id={self.id}, name={self.name!r})>"


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------

class Run(Base):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    site_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("sites.id"), nullable=False
    )
    run_name: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    git_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    data_sources: Mapped[str | None] = mapped_column(String, nullable=True)
    weather_year: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)

    site: Mapped["Site"] = relationship(back_populates="runs")
    inputs: Mapped["RunInput"] = relationship(
        back_populates="run", uselist=False
    )
    results: Mapped["RunResult"] = relationship(
        back_populates="run", uselist=False
    )

    def __repr__(self) -> str:
        return f"<Run(id={self.id}, run_name={self.run_name!r}, status={self.status!r})>"


# ---------------------------------------------------------------------------
# run_inputs
# ---------------------------------------------------------------------------

class RunInput(Base):
    __tablename__ = "run_inputs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False, unique=True
    )

    # Site parameters
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    bess_dispatch_required: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    bess_optimization_required: Mapped[float | None] = mapped_column(Float, nullable=True)

    # System sizing
    dc_size_mw: Mapped[float] = mapped_column(Float, nullable=False)
    ac_installed_mw: Mapped[float] = mapped_column(Float, nullable=False)
    ac_poi_mw: Mapped[float] = mapped_column(Float, nullable=False)

    # Array configuration
    racking: Mapped[str] = mapped_column(String, nullable=False)
    tilt: Mapped[float] = mapped_column(Float, nullable=False)
    azimuth: Mapped[float] = mapped_column(Float, nullable=False)
    module_orientation: Mapped[str] = mapped_column(String, nullable=False)
    number_of_modules: Mapped[int] = mapped_column(Integer, nullable=False)
    ground_clearance_height_m: Mapped[float] = mapped_column(Float, nullable=False)

    # Equipment
    panel_model: Mapped[str] = mapped_column(String, nullable=False)
    bifacial: Mapped[bool] = mapped_column(Boolean, nullable=False)
    inverter_model: Mapped[str] = mapped_column(String, nullable=False)

    # Loss parameters
    gcr: Mapped[float] = mapped_column(Float, nullable=False)
    shading_pct: Mapped[float] = mapped_column(Float, nullable=False)
    dc_wiring_loss_pct: Mapped[float] = mapped_column(Float, nullable=False)
    ac_wiring_loss_pct: Mapped[float] = mapped_column(Float, nullable=False)
    transformer_losses_pct: Mapped[float] = mapped_column(Float, nullable=False)
    degradation_pct: Mapped[float] = mapped_column(Float, nullable=False)
    availability_pct: Mapped[float] = mapped_column(Float, nullable=False)
    module_mismatch_pct: Mapped[float] = mapped_column(Float, nullable=False)
    lid_pct: Mapped[float] = mapped_column(Float, nullable=False)

    # BESS input parameters
    bess_power_mw: Mapped[float | None] = mapped_column(Float, nullable=True)
    bess_duration_hr: Mapped[float | None] = mapped_column(Float, nullable=True)
    bess_rte_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    bess_min_soc_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    bess_max_soc_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    bess_strategy: Mapped[str | None] = mapped_column(String, nullable=True)
    bess_installed_cost_per_kwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    bess_cycles_warranty: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Bill calculation parameters
    bill_calculation: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    rate_file_path: Mapped[str | None] = mapped_column(String, nullable=True)
    utility_name: Mapped[str | None] = mapped_column(String, nullable=True)
    tariff_name: Mapped[str | None] = mapped_column(String, nullable=True)
    load_profile_path: Mapped[str | None] = mapped_column(String, nullable=True)
    load_type: Mapped[str | None] = mapped_column(String, nullable=True)
    annual_consumption_kwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    peak_demand_kw: Mapped[float | None] = mapped_column(Float, nullable=True)

    run: Mapped["Run"] = relationship(back_populates="inputs")

    def __repr__(self) -> str:
        return f"<RunInput(id={self.id}, run_id={self.run_id})>"


# ---------------------------------------------------------------------------
# run_results
# ---------------------------------------------------------------------------

class RunResult(Base):
    __tablename__ = "run_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False, unique=True
    )

    # Key metrics
    annual_energy_mwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_capacity_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    capacity_factor_dc: Mapped[float | None] = mapped_column(Float, nullable=True)
    capacity_factor_ac: Mapped[float | None] = mapped_column(Float, nullable=True)
    specific_yield: Mapped[float | None] = mapped_column(Float, nullable=True)
    performance_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    annual_dc_nominal: Mapped[float | None] = mapped_column(Float, nullable=True)
    annual_snow_loss_pct: Mapped[float | None] = mapped_column(Float, nullable=True)

    # NSRDB bias correction (nullable for Solcast/skipped runs)
    bias_correction_applied: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    bias_correction_model_version: Mapped[str | None] = mapped_column(String, nullable=True)
    mean_ghi_correction_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    mean_dni_correction_factor: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Subhourly resolution correction (nullable for older/skipped runs)
    subhourly_correction_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    subhourly_model_version: Mapped[str | None] = mapped_column(String, nullable=True)
    raw_annual_energy_mwh: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Monthly and detailed data (JSON)
    monthly_energy: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    monthly_snow_loss: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    loss_data: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Bill savings (JSON)
    bill_savings: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # BESS dispatch results (JSON)
    bess_dispatch: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # File paths
    timeseries_file_path: Mapped[str | None] = mapped_column(String, nullable=True)
    report_file_path: Mapped[str | None] = mapped_column(String, nullable=True)
    climate_file_path: Mapped[str | None] = mapped_column(String, nullable=True)

    run: Mapped["Run"] = relationship(back_populates="results")

    def __repr__(self) -> str:
        return f"<RunResult(id={self.id}, run_id={self.run_id})>"


# ---------------------------------------------------------------------------
# buildability_runs
# ---------------------------------------------------------------------------

class BuildabilityRun(Base):
    __tablename__ = "buildability_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_name: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    radius_km: Mapped[float | None] = mapped_column(Float, nullable=True)
    kmz_file_path: Mapped[str | None] = mapped_column(String, nullable=True)
    total_area_acres: Mapped[float] = mapped_column(Float, nullable=False)
    buildable_acres: Mapped[float] = mapped_column(Float, nullable=False)
    buildable_pct: Mapped[float] = mapped_column(Float, nullable=False)
    soft_exclusion_acres: Mapped[float] = mapped_column(Float, nullable=False)
    hard_exclusion_acres: Mapped[float] = mapped_column(Float, nullable=False)
    json_output_path: Mapped[str] = mapped_column(String, nullable=False)
    report_output_path: Mapped[str | None] = mapped_column(String, nullable=True)
    figure_dir_path: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<BuildabilityRun(id={self.id}, run_name={self.run_name!r}, "
            f"source={self.source!r})>"
        )
