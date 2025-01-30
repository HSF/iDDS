#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

"""update transforms and processings to add unique constraint

Revision ID: 8fe5cab9bde1
Revises: a844dae57021
Create Date: 2025-01-29 13:58:00.671713+00:00

"""
from alembic import op
from alembic import context


# revision identifiers, used by Alembic.
revision = '8fe5cab9bde1'
down_revision = 'a844dae57021'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.create_unique_constraint('TRANSFORMS_NAME_UQ', 'transforms', ['request_id', 'name'], schema=schema)
        op.create_unique_constraint('PROCESSINGS_ID_UQ', 'processings', ['request_id', 'transform_id'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_constraint('TRANSFORMS_NAME_UQ', table_name='transforms', schema=schema)
        op.drop_constraint('PROCESSINGS_ID_UQ', table_name='processings', schema=schema)
