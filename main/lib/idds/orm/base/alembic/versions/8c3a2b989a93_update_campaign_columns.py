#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""update campaign columns

Revision ID: 8c3a2b989a93
Revises: ad6d53a69afa
Create Date: 2025-05-22 12:55:09.762195+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import (SCOPE_LENGTH, NAME_LENGTH)


# revision identifiers, used by Alembic.
revision = '8c3a2b989a93'
down_revision = 'ad6d53a69afa'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('campaign_scope', sa.String(SCOPE_LENGTH)), schema=schema)
        op.alter_column('requests', 'campaign_group', type_=sa.String(NAME_LENGTH), schema=schema)
        op.alter_column('requests', 'campaign_tag', type_=sa.String(100), schema=schema)

        op.add_column('requests_group', sa.Column('total_requests', sa.Integer()), schema=schema)
        op.add_column('requests_group', sa.Column('finished_requests', sa.Integer()), schema=schema)
        op.add_column('requests_group', sa.Column('subfinished_requests', sa.Integer()), schema=schema)
        op.add_column('requests_group', sa.Column('failed_requests', sa.Integer()), schema=schema)
        op.add_column('requests_group', sa.Column('processing_requests', sa.Integer()), schema=schema)

        op.add_column('requests', sa.Column('total_transforms', sa.Integer()), schema=schema)
        op.add_column('requests', sa.Column('finished_transforms', sa.Integer()), schema=schema)
        op.add_column('requests', sa.Column('subfinished_transforms', sa.Integer()), schema=schema)
        op.add_column('requests', sa.Column('failed_transforms', sa.Integer()), schema=schema)
        op.add_column('requests', sa.Column('processing_transforms', sa.Integer()), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('requests', 'campaign_scope', schema=schema)
        op.alter_column('requests', 'campaign_group', type_=sa.String(250), schema=schema)
        op.alter_column('requests', 'campaign_tag', type_=sa.String(20), schema=schema)

        op.drop_column('requests_group', 'total_requests', schema=schema)
        op.drop_column('requests_group', 'finished_requests', schema=schema)
        op.drop_column('requests_group', 'subfinished_requests', schema=schema)
        op.drop_column('requests_group', 'failed_requests', schema=schema)
        op.add_column('requests_group', 'processing_requests', schema=schema)

        op.drop_column('requests', 'total_transforms', schema=schema)
        op.drop_column('requests', 'finished_transforms', schema=schema)
        op.drop_column('requests', 'subfinished_transforms', schema=schema)
        op.drop_column('requests', 'failed_transforms', schema=schema)
        op.drop_column('requests', 'processing_transforms', schema=schema)
