#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""add fetch status in contents_update

Revision ID: 5e0aa2aa1fa3
Revises:
Create Date: 2023-03-31 12:36:39.089965+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import ContentFetchStatus
from idds.orm.base.types import EnumWithValue

# revision identifiers, used by Alembic.
revision = '5e0aa2aa1fa3'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.add_column('contents_update', sa.Column('fetch_status', EnumWithValue(ContentFetchStatus), server_default='0', nullable=False), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.drop_column('contents_update', 'fetch_status', schema=schema)
