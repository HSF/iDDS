#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""create contents dep index

Revision ID: 6ae1f334bf41
Revises: 6931b48500a2
Create Date: 2025-03-19 14:53:30.755842+00:00

"""
from alembic import op
from alembic import context


# revision identifiers, used by Alembic.
revision = '6ae1f334bf41'
down_revision = '6931b48500a2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.create_index("CONTENTS_REQ_TF_DEP_ID", "contents", ['content_dep_id', 'request_id', 'transform_id'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_index("CONTENTS_REQ_TF_DEP_ID", "contents", schema=schema)
