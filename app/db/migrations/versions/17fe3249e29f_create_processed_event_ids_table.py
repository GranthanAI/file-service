"""create processed_event_ids table

Revision ID: 17fe3249e29f
Revises: 51a8645188fe
Create Date: 2026-09-09 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '17fe3249e29f'
down_revision: Union[str, None] = '51a8645188fe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'processed_event_ids',
        sa.Column('event_id', sa.UUID(), nullable=False),
        sa.Column('consumer', sa.String(length=100), nullable=False),
        sa.Column('processed_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('event_id', 'consumer'),
    )


def downgrade() -> None:
    op.drop_table('processed_event_ids')
