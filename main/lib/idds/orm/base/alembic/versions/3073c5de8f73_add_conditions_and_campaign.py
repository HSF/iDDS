#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

"""add conditions and campaign

Revision ID: 3073c5de8f73
Revises: 40ead97e63c6
Create Date: 2024-08-05 13:21:37.265614+00:00

"""
import datetime

from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import ConditionStatus
from idds.orm.base.types import EnumWithValue
from idds.orm.base.types import JSON

# revision identifiers, used by Alembic.
revision = '3073c5de8f73'
down_revision = '40ead97e63c6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('campaign', sa.String(100)), schema=schema)
        op.add_column('requests', sa.Column('campaign_group', sa.String(250)), schema=schema)
        op.add_column('requests', sa.Column('campaign_tag', sa.String(20)), schema=schema)

        op.add_column('transforms', sa.Column('internal_id', sa.String(20)), schema=schema)
        op.add_column('transforms', sa.Column('has_previous_conditions', sa.Integer()), schema=schema)
        op.add_column('transforms', sa.Column('loop_index', sa.Integer()), schema=schema)
        op.add_column('transforms', sa.Column('cloned_from', sa.BigInteger()), schema=schema)
        op.add_column('transforms', sa.Column('triggered_conditions', JSON()), schema=schema)
        op.add_column('transforms', sa.Column('untriggered_conditions', JSON()), schema=schema)

        op.create_table('conditions',
                        sa.Column('condition_id', sa.BigInteger(), sa.Sequence('CONDITION_ID_SEQ', schema=schema)),
                        sa.Column('request_id', sa.BigInteger(), nullable=False),
                        sa.Column('internal_id', sa.String(20), nullable=False),
                        sa.Column('status', EnumWithValue(ConditionStatus), nullable=False),
                        sa.Column('is_loop', sa.Integer()),
                        sa.Column('loop_index', sa.Integer()),
                        sa.Column('cloned_from', sa.BigInteger()),
                        sa.Column("created_at", sa.DateTime, default=datetime.datetime.utcnow, nullable=False),
                        sa.Column("updated_at", sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False),
                        sa.Column("evaluate_result", sa.String(200)),
                        sa.Column('previous_transforms', JSON()),
                        sa.Column('following_transforms', JSON()),
                        sa.Column('condition', JSON()),
                        schema=schema)
        op.create_primary_key('CONDITION_PK', 'conditions', ['condition_id'], schema=schema)
        op.create_unique_constraint('CONDITION_ID_UQ', 'conditions', ['request_id', 'internal_id'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('requests', 'campaign', schema=schema)
        op.drop_column('requests', 'campaign_group', schema=schema)
        op.drop_column('requests', 'campaign_tag', schema=schema)

        op.drop_column('transforms', 'internal_id', schema=schema)
        op.drop_column('transforms', 'has_previous_conditions', schema=schema)
        op.drop_column('transforms', 'loop_index', schema=schema)
        op.drop_column('transforms', 'cloned_from', schema=schema)
        op.drop_column('transforms', 'triggered_conditions', schema=schema)
        op.drop_column('transforms', 'untriggered_conditions', schema=schema)

        op.drop_constraint('CONDITION_ID_UQ', table_name='conditions', schema=schema)
        op.drop_constraint('CONDITION_PK', table_name='conditions', schema=schema)
        op.drop_table('conditions', schema=schema)
