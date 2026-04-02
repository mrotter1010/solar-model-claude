"""add bess, bill, economics, and ftm columns to run_inputs and run_results

Revision ID: c3d4e5f6a7b8
Revises: 4a55fe96bebe
Create Date: 2026-04-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = '4a55fe96bebe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add missing columns to run_inputs and run_results; fix type mismatches."""

    # ------------------------------------------------------------------
    # run_inputs: fix type mismatches (Float → Boolean)
    # ------------------------------------------------------------------
    op.alter_column(
        'run_inputs', 'bess_dispatch_required',
        type_=sa.Boolean(),
        existing_type=sa.Float(),
        nullable=False,
        existing_nullable=True,
        server_default=sa.text('false'),
        postgresql_using='COALESCE(bess_dispatch_required, 0)::int::boolean',
    )
    op.alter_column(
        'run_inputs', 'bess_optimization_required',
        type_=sa.Boolean(),
        existing_type=sa.Float(),
        nullable=False,
        existing_nullable=True,
        server_default=sa.text('false'),
        postgresql_using='COALESCE(bess_optimization_required, 0)::int::boolean',
    )

    # ------------------------------------------------------------------
    # run_inputs: BESS dispatch parameters (10 columns)
    # ------------------------------------------------------------------
    op.add_column('run_inputs', sa.Column('bess_power_mw', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_duration_hr', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_rte_percent', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_min_soc_percent', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_max_soc_percent', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_strategy', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_installed_cost_per_kwh', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_cycles_warranty', sa.Integer(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_solar_only_charging', sa.Boolean(), nullable=False, server_default=sa.text('false')))
    op.add_column('run_inputs', sa.Column('bess_grid_only_charging', sa.Boolean(), nullable=False, server_default=sa.text('false')))

    # ------------------------------------------------------------------
    # run_inputs: BESS sizing optimization parameters (5 columns)
    # ------------------------------------------------------------------
    op.add_column('run_inputs', sa.Column('bess_power_min_mw', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_power_max_mw', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_duration_min_hr', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_duration_max_hr', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('bess_opex_per_kw_year', sa.Float(), nullable=True))

    # ------------------------------------------------------------------
    # run_inputs: project economics parameters (6 columns)
    # ------------------------------------------------------------------
    op.add_column('run_inputs', sa.Column('discount_rate_pct', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('project_lifetime_years', sa.Integer(), nullable=True))
    op.add_column('run_inputs', sa.Column('rate_escalation_pct', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('solar_cost_per_kw_dc', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('solar_cost_per_kw_ac', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('solar_opex_per_kw_dc_year', sa.Float(), nullable=True))

    # ------------------------------------------------------------------
    # run_inputs: bill calculation parameters (8 columns)
    # ------------------------------------------------------------------
    op.add_column('run_inputs', sa.Column('bill_calculation', sa.Boolean(), nullable=True))
    op.add_column('run_inputs', sa.Column('rate_file_path', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('utility_name', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('tariff_name', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('load_profile_path', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('load_type', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('annual_consumption_kwh', sa.Float(), nullable=True))
    op.add_column('run_inputs', sa.Column('peak_demand_kw', sa.Float(), nullable=True))

    # ------------------------------------------------------------------
    # run_inputs: FTM / wholesale dispatch parameters (6 columns)
    # ------------------------------------------------------------------
    op.add_column('run_inputs', sa.Column('dispatch_mode', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('iso', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('lmp_zone', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('lmp_market', sa.String(), nullable=True))
    op.add_column('run_inputs', sa.Column('lmp_year', sa.Integer(), nullable=True))
    op.add_column('run_inputs', sa.Column('ancillary_revenue_per_kw_year', sa.Float(), nullable=True))

    # ------------------------------------------------------------------
    # run_results: BESS, bill, and FTM result columns (5 columns)
    # ------------------------------------------------------------------
    op.add_column('run_results', sa.Column('bill_savings', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('run_results', sa.Column('bess_dispatch', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('run_results', sa.Column('bess_sizing', postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.add_column('run_results', sa.Column('lmp_data', sa.Text(), nullable=True))
    op.add_column('run_results', sa.Column('ftm_economics', sa.Text(), nullable=True))


def downgrade() -> None:
    """Reverse all changes: drop added columns, revert type changes."""

    # ------------------------------------------------------------------
    # run_results: drop 5 columns
    # ------------------------------------------------------------------
    op.drop_column('run_results', 'ftm_economics')
    op.drop_column('run_results', 'lmp_data')
    op.drop_column('run_results', 'bess_sizing')
    op.drop_column('run_results', 'bess_dispatch')
    op.drop_column('run_results', 'bill_savings')

    # ------------------------------------------------------------------
    # run_inputs: drop FTM columns (6)
    # ------------------------------------------------------------------
    op.drop_column('run_inputs', 'ancillary_revenue_per_kw_year')
    op.drop_column('run_inputs', 'lmp_year')
    op.drop_column('run_inputs', 'lmp_market')
    op.drop_column('run_inputs', 'lmp_zone')
    op.drop_column('run_inputs', 'iso')
    op.drop_column('run_inputs', 'dispatch_mode')

    # ------------------------------------------------------------------
    # run_inputs: drop bill columns (8)
    # ------------------------------------------------------------------
    op.drop_column('run_inputs', 'peak_demand_kw')
    op.drop_column('run_inputs', 'annual_consumption_kwh')
    op.drop_column('run_inputs', 'load_type')
    op.drop_column('run_inputs', 'load_profile_path')
    op.drop_column('run_inputs', 'tariff_name')
    op.drop_column('run_inputs', 'utility_name')
    op.drop_column('run_inputs', 'rate_file_path')
    op.drop_column('run_inputs', 'bill_calculation')

    # ------------------------------------------------------------------
    # run_inputs: drop economics columns (6)
    # ------------------------------------------------------------------
    op.drop_column('run_inputs', 'solar_opex_per_kw_dc_year')
    op.drop_column('run_inputs', 'solar_cost_per_kw_ac')
    op.drop_column('run_inputs', 'solar_cost_per_kw_dc')
    op.drop_column('run_inputs', 'rate_escalation_pct')
    op.drop_column('run_inputs', 'project_lifetime_years')
    op.drop_column('run_inputs', 'discount_rate_pct')

    # ------------------------------------------------------------------
    # run_inputs: drop BESS optimization columns (5)
    # ------------------------------------------------------------------
    op.drop_column('run_inputs', 'bess_opex_per_kw_year')
    op.drop_column('run_inputs', 'bess_duration_max_hr')
    op.drop_column('run_inputs', 'bess_duration_min_hr')
    op.drop_column('run_inputs', 'bess_power_max_mw')
    op.drop_column('run_inputs', 'bess_power_min_mw')

    # ------------------------------------------------------------------
    # run_inputs: drop BESS dispatch columns (10)
    # ------------------------------------------------------------------
    op.drop_column('run_inputs', 'bess_grid_only_charging')
    op.drop_column('run_inputs', 'bess_solar_only_charging')
    op.drop_column('run_inputs', 'bess_cycles_warranty')
    op.drop_column('run_inputs', 'bess_installed_cost_per_kwh')
    op.drop_column('run_inputs', 'bess_strategy')
    op.drop_column('run_inputs', 'bess_max_soc_percent')
    op.drop_column('run_inputs', 'bess_min_soc_percent')
    op.drop_column('run_inputs', 'bess_rte_percent')
    op.drop_column('run_inputs', 'bess_duration_hr')
    op.drop_column('run_inputs', 'bess_power_mw')

    # ------------------------------------------------------------------
    # run_inputs: revert type changes (Boolean → Float)
    # ------------------------------------------------------------------
    op.alter_column(
        'run_inputs', 'bess_optimization_required',
        type_=sa.Float(),
        existing_type=sa.Boolean(),
        nullable=True,
        existing_nullable=False,
        server_default=None,
        postgresql_using='bess_optimization_required::double precision',
    )
    op.alter_column(
        'run_inputs', 'bess_dispatch_required',
        type_=sa.Float(),
        existing_type=sa.Boolean(),
        nullable=True,
        existing_nullable=False,
        server_default=None,
        postgresql_using='bess_dispatch_required::double precision',
    )
