#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""add meta info table

Revision ID: 354f8e5a5879
Revises: 1bc6e82e8514
Create Date: 2024-01-09 10:26:54.783489+00:00

"""

import datetime

from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import MetaStatus
from idds.orm.base.types import EnumWithValue
from idds.orm.base.types import JSON

# revision identifiers, used by Alembic.
revision = '354f8e5a5879'
down_revision = '1bc6e82e8514'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.create_table('meta_info',
                        sa.Column('meta_id', sa.BigInteger(), sa.Sequence('METAINFO_ID_SEQ', schema=schema)),
                        sa.Column('name', sa.String(50), nullable=False),
                        sa.Column('status', EnumWithValue(MetaStatus), nullable=False),
                        sa.Column("created_at", sa.DateTime, default=datetime.datetime.utcnow, nullable=False),
                        sa.Column("updated_at", sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False),
                        sa.Column("description", sa.String(1000)),
                        sa.Column('meta_info', JSON()),
                        schema=schema)
        op.create_primary_key('METAINFO_PK', 'meta_info', ['meta_id'], schema=schema)
        op.create_unique_constraint('METAINFO_NAME_UQ', 'meta_info', ['name'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.drop_constraint('METAINFO_NAME_UQ', table_name='meta_info', schema=schema)
        op.drop_constraint('METAINFO_PK', table_name='meta_info', schema=schema)
        op.drop_table('meta_info', schema=schema)
