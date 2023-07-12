#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""add external content_id

Revision ID: f79663a7e94e
Revises: 0204f391c32d
Create Date: 2023-06-22 11:36:20.664961+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f79663a7e94e'
down_revision = '0204f391c32d'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.add_column('contents', sa.Column('external_coll_id', sa.BigInteger()), schema=schema)
        op.add_column('contents', sa.Column('external_content_id', sa.BigInteger()), schema=schema)
        op.add_column('contents', sa.Column('external_event_id', sa.BigInteger()), schema=schema)
        op.add_column('contents', sa.Column('external_event_status', sa.Integer()), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.drop_column('contents', 'external_coll_id', schema=schema)
        op.drop_column('contents', 'external_content_id', schema=schema)
        op.drop_column('contents', 'external_event_id', schema=schema)
        op.drop_column('contents', 'external_event_status', schema=schema)
