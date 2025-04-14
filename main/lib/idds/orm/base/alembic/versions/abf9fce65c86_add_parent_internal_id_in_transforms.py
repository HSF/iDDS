#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""add parent_internal_id in transforms

Revision ID: abf9fce65c86
Revises: 6ae1f334bf41
Create Date: 2025-04-11 08:37:56.638051+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'abf9fce65c86'
down_revision = '6ae1f334bf41'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('additional_data_storage', sa.String(512)), schema=schema)
        op.add_column('transforms', sa.Column('parent_internal_id', sa.String(20)), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('requests', 'additional_data_storage', schema=schema)
        op.drop_column('transforms', 'parent_internal_id', schema=schema)
