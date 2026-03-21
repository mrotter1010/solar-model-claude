"""SQLAlchemy ORM model for bill calculation results."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.database.models import Base


class BillCalculationRun(Base):
    """Persisted bill calculation results for a pipeline run."""

    __tablename__ = "bill_calculation_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("runs.id"), nullable=False, unique=True
    )

    # Rate source
    rate_source: Mapped[str] = mapped_column(String, nullable=False)
    utility_name: Mapped[str | None] = mapped_column(String, nullable=True)
    tariff_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Load source
    load_source: Mapped[str] = mapped_column(String, nullable=False)
    building_type: Mapped[str | None] = mapped_column(String, nullable=True)

    # Key metrics
    annual_consumption_kwh: Mapped[float] = mapped_column(Float, nullable=False)
    annual_production_kwh: Mapped[float] = mapped_column(Float, nullable=False)
    annual_bill_without_solar: Mapped[float] = mapped_column(Float, nullable=False)
    annual_bill_with_solar: Mapped[float] = mapped_column(Float, nullable=False)
    annual_savings: Mapped[float] = mapped_column(Float, nullable=False)
    savings_percent: Mapped[float] = mapped_column(Float, nullable=False)
    avoided_cost_per_kwh: Mapped[float] = mapped_column(Float, nullable=False)

    # Monthly breakdown (JSON)
    monthly_detail: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<BillCalculationRun(id={self.id}, run_id={self.run_id}, "
            f"savings=${self.annual_savings:.0f})>"
        )
