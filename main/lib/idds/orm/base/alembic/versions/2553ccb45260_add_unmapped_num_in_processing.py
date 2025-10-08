#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""add unmapped num in processing

Revision ID: 2553ccb45260
Revises: e3f3af2cadc7
Create Date: 2025-09-29 12:56:05.251818+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import CommandType
from idds.orm.base.types import EnumWithValue

# revision identifiers, used by Alembic.
revision = '2553ccb45260'
down_revision = 'e3f3af2cadc7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('command', EnumWithValue(CommandType), server_default=sa.text("0")), schema=schema)
        op.add_column('transforms', sa.Column('command', EnumWithValue(CommandType), server_default=sa.text("0")), schema=schema)
        op.add_column('processings', sa.Column('command', EnumWithValue(CommandType), server_default=sa.text("0")), schema=schema)

        op.alter_column('transforms', 'parent_internal_id', type_=sa.String(400), schema=schema)

        op.add_column('processings', sa.Column('num_unmapped', sa.Integer(), server_default=sa.text("0")), schema=schema)

        op.add_column('processings', sa.Column('loop_index', sa.Integer()), schema=schema)
        op.add_column('processings', sa.Column('internal_id', sa.String(20)), schema=schema)
        op.add_column('processings', sa.Column('parent_internal_id', sa.String(400)), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('processings', 'num_unmapped', schema=schema)

        op.drop_column('processings', 'command', schema=schema)
        op.drop_column('transforms', 'command', schema=schema)
        op.drop_column('requests', 'command', schema=schema)

        op.drop_column('processings', 'loop_index', schema=schema)
        op.drop_column('processings', 'internal_id', schema=schema)
        op.drop_column('processings', 'parent_internal_id', schema=schema)
