#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2026

"""add run_id in transforms and processings

Revision ID: a1b2c3d4e5f6
Revises: 2553ccb45260
Create Date: 2026-04-06 00:00:00.000000+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = '2553ccb45260'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('transforms', sa.Column('run_id', sa.String(64)), schema=schema)
        op.add_column('processings', sa.Column('run_id', sa.String(64)), schema=schema)

        op.create_index('TRANSFORMS_RUN_ID_IDX', 'transforms', ['run_id'], schema=schema)
        op.create_index('PROCESSINGS_RUN_ID_IDX', 'processings', ['run_id'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_index('TRANSFORMS_RUN_ID_IDX', table_name='transforms', schema=schema)
        op.drop_index('PROCESSINGS_RUN_ID_IDX', table_name='processings', schema=schema)

        op.drop_column('transforms', 'run_id', schema=schema)
        op.drop_column('processings', 'run_id', schema=schema)
