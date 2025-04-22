#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""add cloud info in requests

Revision ID: ad6d53a69afa
Revises: abf9fce65c86
Create Date: 2025-04-22 11:34:37.482593+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ad6d53a69afa'
down_revision = 'abf9fce65c86'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('cloud', sa.String(50)), schema=schema)
        op.add_column('requests', sa.Column('queue', sa.String(50)), schema=schema)

        op.add_column('requests_group', sa.Column('cloud', sa.String(50)), schema=schema)
        op.add_column('requests_group', sa.Column('queue', sa.String(50)), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('requests', 'cloud', schema=schema)
        op.drop_column('requests', 'queue', schema=schema)

        op.drop_column('requests_group', 'cloud', schema=schema)
        op.drop_column('requests_group', 'queue', schema=schema)
