#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""update message null constraints

Revision ID: 6ca0e5e466eb
Revises: 5e0aa2aa1fa3
Create Date: 2023-04-25 16:58:29.975397+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6ca0e5e466eb'
down_revision = '5e0aa2aa1fa3'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.alter_column('messages', sa.Column('request_id'), nullable=True, schema=schema)
        op.alter_column('messages', sa.Column('transform_id'), nullable=True, schema=schema)
        op.alter_column('messages', sa.Column('processing_id'), nullable=True, schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.alter_column('messages', sa.Column('request_id'), nullable=False, schema=schema)
        op.alter_column('messages', sa.Column('transform_id'), nullable=False, schema=schema)
        op.alter_column('messages', sa.Column('processing_id'), nullable=False, schema=schema)
