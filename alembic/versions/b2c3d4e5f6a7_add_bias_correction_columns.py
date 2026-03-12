"""add bias correction columns

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-11 23:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add NSRDB bias correction columns to run_results."""
    op.add_column('run_results', sa.Column('bias_correction_applied', sa.Boolean(), nullable=True))
    op.add_column('run_results', sa.Column('bias_correction_model_version', sa.String(), nullable=True))
    op.add_column('run_results', sa.Column('mean_ghi_correction_factor', sa.Float(), nullable=True))
    op.add_column('run_results', sa.Column('mean_dni_correction_factor', sa.Float(), nullable=True))


def downgrade() -> None:
    """Remove NSRDB bias correction columns from run_results."""
    op.drop_column('run_results', 'mean_dni_correction_factor')
    op.drop_column('run_results', 'mean_ghi_correction_factor')
    op.drop_column('run_results', 'bias_correction_model_version')
    op.drop_column('run_results', 'bias_correction_applied')
