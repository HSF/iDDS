#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""add sub_map_id

Revision ID: b0ec813021d6
Revises: f79663a7e94e
Create Date: 2023-06-22 11:46:41.634551+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b0ec813021d6'
down_revision = 'f79663a7e94e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.add_column('contents', sa.Column('sub_map_id', sa.BigInteger()), schema=schema)
        op.add_column('contents', sa.Column('dep_sub_map_id', sa.BigInteger()), schema=schema)
        try:
            op.drop_constraint(constraint_name="CONTENT_ID_UQ", table_name="contents", schema=schema)
        except Exception as ex:
            print(ex)
        op.create_unique_constraint('CONTENT_ID_UQ', 'contents', ['transform_id', 'coll_id', 'map_id', 'sub_map_id', 'dep_sub_map_id', 'content_relation_type', 'name', 'min_id', 'max_id'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.drop_constraint(constraint_name="CONTENT_ID_UQ", table_name="contents", schema=schema)
        op.drop_column('contents', 'sub_map_id', schema=schema)
        op.drop_column('contents', 'dep_sub_map_id', schema=schema)
        op.create_unique_constraint('CONTENT_ID_UQ', 'contents', ['transform_id', 'coll_id', 'map_id', 'name', 'min_id', 'max_id'], schema=schema)
