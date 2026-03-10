"""add subhourly correction columns

Revision ID: a1b2c3d4e5f6
Revises: 7f83e962a6cf
Create Date: 2026-03-10 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '7f83e962a6cf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add subhourly resolution correction columns to run_results."""
    op.add_column('run_results', sa.Column('subhourly_correction_pct', sa.Float(), nullable=True))
    op.add_column('run_results', sa.Column('subhourly_model_version', sa.String(), nullable=True))
    op.add_column('run_results', sa.Column('raw_annual_energy_mwh', sa.Float(), nullable=True))


def downgrade() -> None:
    """Remove subhourly resolution correction columns from run_results."""
    op.drop_column('run_results', 'raw_annual_energy_mwh')
    op.drop_column('run_results', 'subhourly_model_version')
    op.drop_column('run_results', 'subhourly_correction_pct')
