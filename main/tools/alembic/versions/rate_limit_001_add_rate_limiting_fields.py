"""Add rate limiting fields to Throttler table

Revision ID: rate_limit_001
Revises: 
Create Date: 2025-12-27 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'rate_limit_001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Add rate limiting columns to throttlers table."""
    # Add new columns with defaults for existing rows
    op.add_column('throttlers', sa.Column('max_requests_per_minute', sa.Integer(), nullable=False, server_default='100'))
    op.add_column('throttlers', sa.Column('max_requests_per_hour', sa.Integer(), nullable=False, server_default='10000'))
    op.add_column('throttlers', sa.Column('max_burst', sa.Integer(), nullable=False, server_default='50'))
    op.add_column('throttlers', sa.Column('priority_level', sa.Integer(), nullable=False, server_default='5'))
    op.add_column('throttlers', sa.Column('vo_name', sa.String(50), nullable=True))
    
    # Create index on vo_name for faster lookups
    op.create_index('idx_throttlers_vo_name', 'throttlers', ['vo_name'])


def downgrade():
    """Remove rate limiting columns from throttlers table."""
    op.drop_index('idx_throttlers_vo_name')
    op.drop_column('throttlers', 'vo_name')
    op.drop_column('throttlers', 'priority_level')
    op.drop_column('throttlers', 'max_burst')
    op.drop_column('throttlers', 'max_requests_per_hour')
    op.drop_column('throttlers', 'max_requests_per_minute')
