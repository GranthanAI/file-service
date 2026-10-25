"""create youtube_sources table

Revision ID: a7c3e1d94b20
Revises: 17fe3249e29f
Create Date: 2026-10-25 00:00:00.000000

Additive: one new table, no change to `files` or any existing row. Rollback drops only this table; the File rows of attached
videos remain (they would then be ordinary documents without video metadata), so remove the sources first if that matters.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a7c3e1d94b20'
down_revision: Union[str, None] = '17fe3249e29f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'youtube_sources',
        sa.Column('document_id', sa.UUID(), sa.ForeignKey('files.document_id'), primary_key=True),
        sa.Column('owner_id', sa.UUID(), nullable=False),
        sa.Column('video_id', sa.String(length=11), nullable=False),
        sa.Column('canonical_url', sa.String(length=100), nullable=False),
        sa.Column('title', sa.String(length=300), nullable=True),
        sa.Column('language', sa.String(length=35), nullable=True),
        sa.Column('language_name', sa.String(length=100), nullable=True),
        sa.Column('is_generated', sa.Boolean(), nullable=True),
        sa.Column('provider', sa.String(length=60), nullable=True),
        sa.Column('transcript_version', sa.Integer(), server_default='0', nullable=False),
        sa.Column('transcript_sha256', sa.String(length=64), nullable=True),
        sa.Column('segment_count', sa.Integer(), nullable=True),
        sa.Column('duration_seconds', sa.Float(), nullable=True),
        sa.Column('extraction_status', sa.String(length=20), server_default='PENDING', nullable=False),
        sa.Column('error_code', sa.String(length=40), nullable=True),
        sa.Column('attempts', sa.Integer(), server_default='0', nullable=False),
        sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
        sa.Column('next_retry_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('removed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.CheckConstraint(
            "extraction_status IN ('PENDING','FETCHING','FETCHED','FAILED_RETRYABLE','FAILED_PERMANENT')",
            name='ck_youtube_sources_status',
        ),
        sa.CheckConstraint('char_length(video_id) = 11', name='ck_youtube_sources_video_id_len'),
    )
    op.create_index(
        'uq_youtube_sources_owner_video_live', 'youtube_sources', ['owner_id', 'video_id'],
        unique=True, postgresql_where=sa.text('removed_at IS NULL'),
    )
    op.create_index('idx_youtube_sources_owner_created', 'youtube_sources', ['owner_id', 'created_at'])


def downgrade() -> None:
    op.drop_index('idx_youtube_sources_owner_created', table_name='youtube_sources')
    op.drop_index('uq_youtube_sources_owner_video_live', table_name='youtube_sources')
    op.drop_table('youtube_sources')
