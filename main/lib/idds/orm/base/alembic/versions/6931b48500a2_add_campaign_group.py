#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""add campaign group

Revision ID: 6931b48500a2
Revises: 8fe5cab9bde1
Create Date: 2025-02-21 11:44:59.871046+00:00

"""
import datetime
from alembic import op
from alembic import context
import sqlalchemy as sa

from idds.common.constants import (RequestGroupType, RequestGroupStatus, RequestGroupLocking)
from idds.orm.base.types import JSON, JSONString, EnumWithValue
from idds.common.constants import (SCOPE_LENGTH, NAME_LENGTH)


# revision identifiers, used by Alembic.
revision = '6931b48500a2'
down_revision = '8fe5cab9bde1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        # requests_group table
        op.create_table('requests_group',
                        sa.Column('group_id', sa.BigInteger(), sa.Sequence('REQUEST_GROUP_ID_SEQ', schema=schema)),
                        sa.Column('campaign', sa.String(50), nullable=False, default='Default'),
                        sa.Column('campaign_scope', sa.String(SCOPE_LENGTH), nullable=False, default='Default'),
                        sa.Column('campaign_group', sa.String(NAME_LENGTH), nullable=False),
                        sa.Column('campaign_tag', sa.String(100), nullable=False),
                        sa.Column('requester', sa.String(20)),
                        sa.Column('group_type', EnumWithValue(RequestGroupType), nullable=False),
                        sa.Column('username', sa.String(20)),
                        sa.Column('userdn', sa.String(200)),
                        sa.Column('priority', sa.Integer(), nullable=False, default=50),
                        sa.Column('status', EnumWithValue(RequestGroupStatus), nullable=False),
                        sa.Column('substatus', EnumWithValue(RequestGroupStatus), default=0),
                        sa.Column('oldstatus', EnumWithValue(RequestGroupStatus), default=0),
                        sa.Column('locking', EnumWithValue(RequestGroupLocking), nullable=False),
                        sa.Column("created_at", sa.DateTime, default=datetime.datetime.utcnow, nullable=False),
                        sa.Column("updated_at", sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False),
                        sa.Column("next_poll_at", sa.DateTime, default=datetime.datetime.utcnow),
                        sa.Column("accessed_at", sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
                        sa.Column("expired_at", sa.DateTime),
                        sa.Column('new_retries', sa.Integer(), default=0),
                        sa.Column('update_retries', sa.Integer(), default=0),
                        sa.Column('max_new_retries', sa.Integer(), default=3),
                        sa.Column('max_update_retries', sa.Integer(), default=0),
                        sa.Column('new_poll_period', sa.Interval(), default=datetime.timedelta(seconds=1)),
                        sa.Column('update_poll_period', sa.Interval(), default=datetime.timedelta(seconds=10)),
                        sa.Column('site', sa.String(50)),
                        sa.Column('locking_hostname', sa.String(50)),
                        sa.Column('locking_pid', sa.BigInteger(), autoincrement=False),
                        sa.Column('locking_thread_id', sa.BigInteger(), autoincrement=False),
                        sa.Column('locking_thread_name', sa.String(100)),
                        sa.Column('errors', JSONString(1024)),
                        sa.Column('group_metadata', JSON()),
                        sa.Column('processing_metadata', JSON()),
                        schema=schema)

        op.create_primary_key('REQUESTGROUP_PK', 'requests_group', ['group_id'], schema=schema)
        op.create_unique_constraint('REQUESTGROUP_CM_UQ', 'requests_group', ['campaign', 'campaign_scope', 'campaign_group', 'campaign_tag'], schema=schema)
        op.create_check_constraint('REQUESTGROUP_STATUS_ID_NN', 'requests_group', 'status IS NOT NULL', schema=schema)
        op.create_index('REQUESTGROUP_CM_NAME_IDX', 'requests_group', ['campaign', 'campaign_scope', 'campaign_group', 'campaign_tag'], unique=True, schema=schema)
        op.create_index('REQUESTGROUP_STATUS_SITE', 'requests_group', ['status', 'site', 'group_id'], schema=schema),
        op.create_index('REQUESTGROUP_STATUS_PRIO_IDX', 'requests_group', ['status', 'priority', 'group_id', 'locking', 'updated_at', 'next_poll_at', 'created_at'], schema=schema),
        op.create_index('REQUESTGROUP_STATUS_POLL_IDX', 'requests_group', ['status', 'priority', 'locking', 'updated_at', 'new_poll_period', 'update_poll_period', 'created_at', 'group_id'], schema=schema)

        # requests
        op.alter_column('requests', 'campaign_tag', type_=sa.String(100), schema=schema)
        op.add_column('requests', sa.Column('group_id', sa.BigInteger()), schema=schema)
        op.create_foreign_key('REQUESTS_GROUP_ID_FK', 'requests', 'requests_group', ['group_id'], ['group_id'], source_schema=schema, referent_schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        # requests
        op.alter_column('requests', 'campaign_tag', type_=sa.String(20), schema=schema)
        op.drop_constraint('REQUESTS_GROUP_ID_FK', table_name='requests', schema=schema)
        op.drop_column('requests', 'group_id', schema=schema)

        # requests_group
        op.drop_constraint('REQUESTGROUP_STATUS_ID_NN', table_name='requests_group', schema=schema)
        op.drop_constraint('REQUESTGROUP_CM_UQ', table_name='requests_group', schema=schema)
        op.drop_constraint('REQUESTGROUP_PK', table_name='requests_group', schema=schema)

        op.drop_index('REQUESTGROUP_CM_NAME_IDX', table_name='requests_group', schema=schema)
        op.drop_index('REQUESTGROUP_STATUS_SITE', table_name='requests_group', schema=schema)
        op.drop_index('REQUESTGROUP_STATUS_PRIO_IDX', table_name='requests_group', schema=schema)
        op.drop_index('REQUESTGROUP_STATUS_POLL_IDX', table_name='requests_group', schema=schema)

        op.drop_table('requests_group', schema=schema)
