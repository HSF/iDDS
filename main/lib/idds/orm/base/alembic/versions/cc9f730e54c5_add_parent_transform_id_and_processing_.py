#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

"""add parent_transform_id and processing_type

Revision ID: cc9f730e54c5
Revises: 354f8e5a5879
Create Date: 2024-03-01 14:58:12.471189+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import ProcessingType
from idds.orm.base.types import EnumWithValue

# revision identifiers, used by Alembic.
revision = 'cc9f730e54c5'
down_revision = '354f8e5a5879'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('transforms', sa.Column('parent_transform_id', sa.BigInteger()), schema=schema)
        op.add_column('transforms', sa.Column('previous_transform_id', sa.BigInteger()), schema=schema)
        op.add_column('transforms', sa.Column('current_processing_id', sa.BigInteger()), schema=schema)

        op.add_column('processings', sa.Column('processing_type', EnumWithValue(ProcessingType), server_default='0', nullable=False), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('transforms', 'parent_transform_id', schema=schema)
        op.drop_column('transforms', 'previous_transform_id', schema=schema)
        op.drop_column('transforms', 'current_processing_id', schema=schema)

        op.drop_column('processings', 'processing_type', schema=schema)
