#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""using hash on contents long name

Revision ID: 1bc6e82e8514
Revises: b0ec813021d6
Create Date: 2023-09-27 09:28:37.068476+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1bc6e82e8514'
down_revision = 'b0ec813021d6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        try:
            op.drop_constraint(constraint_name="CONTENT_ID_UQ", table_name="contents", schema=schema)
        except Exception as ex:
            print(ex)
        try:
            op.drop_index(index_name="CONTENTS_ID_NAME_IDX", table_name="contents", schema=schema)
        except Exception as ex:
            print(ex)

        op.create_unique_constraint('CONTENT_ID_UQ', 'contents',
                                    ['transform_id', 'coll_id', 'map_id', 'sub_map_id', 'dep_sub_map_id', 'content_relation_type', sa.func.adler32('name'), sa.func.md5('name'), sa.func.hash('name'), 'min_id', 'max_id'],
                                    schema=schema)
        op.create_index('CONTENTS_ID_NAME_IDX', 'contents', ['coll_id', 'scope', sa.func.hash('name'), 'status'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        try:
            op.drop_constraint(constraint_name="CONTENT_ID_UQ", table_name="contents", schema=schema)
        except Exception as ex:
            print(ex)
        try:
            op.drop_index(index_name="CONTENTS_ID_NAME_IDX", table_name="contents", schema=schema)
        except Exception as ex:
            print(ex)
        op.create_unique_constraint('CONTENT_ID_UQ', 'contents', ['transform_id', 'coll_id', 'map_id', 'sub_map_id', 'dep_sub_map_id', 'content_relation_type', 'name', 'min_id', 'max_id'], schema=schema)
        op.create_index('CONTENTS_ID_NAME_IDX', 'contents', ['coll_id', 'scope', 'name', 'status'], schema=schema)
