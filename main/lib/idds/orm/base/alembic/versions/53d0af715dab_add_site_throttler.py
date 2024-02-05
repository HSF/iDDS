#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""add site throttler

Revision ID: 53d0af715dab
Revises: 6ca0e5e466eb
Create Date: 2023-05-18 10:02:46.858647+00:00

"""

import datetime

from alembic import op
from alembic import context
import sqlalchemy as sa


from idds.common.constants import ThrottlerStatus
from idds.orm.base.types import EnumWithValue
from idds.orm.base.types import JSON

# revision identifiers, used by Alembic.
revision = '53d0af715dab'
down_revision = '6ca0e5e466eb'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('site', sa.String(50)), schema=schema)
        op.create_index("REQUESTS_STATUS_SITE", "requests", ['status', 'site', 'request_id'], schema=schema)

        op.add_column('transforms', sa.Column('site', sa.String(50)), schema=schema)
        op.create_index("TRANSFORMS_STATUS_SITE", "transforms", ['status', 'site', 'request_id', 'transform_id'], schema=schema)

        op.add_column('processings', sa.Column('site', sa.String(50)), schema=schema)
        op.create_index("PROCESSINGS_STATUS_SITE", "processings", ['status', 'site', 'request_id', 'transform_id', 'processing_id'], schema=schema)

        op.add_column('messages', sa.Column('fetching_id', sa.Integer()), schema=schema)

        try:
            op.drop_constraint('THROTTLER_SITE_UQ', table_name='throttlers', schema=schema)
        except:
            pass
        try:
            op.drop_constraint('THROTTLER_PK', table_name='throttlers', schema=schema)
        except:
            pass
        # op.create_sequence(sa.Sequence('THROTTLER_ID_SEQ', schema=schema))
        op.execute(sa.schema.CreateSequence(sa.Sequence('THROTTLER_ID_SEQ', schema=schema)))
        op.create_table('throttlers',
                        sa.Column('throttler_id', sa.BigInteger(), sa.Sequence('THROTTLER_ID_SEQ', schema=schema)),
                        sa.Column('site', sa.String(50), nullable=False),
                        sa.Column('status', EnumWithValue(ThrottlerStatus), nullable=False),
                        sa.Column('num_requests', sa.Integer()),
                        sa.Column('num_transforms', sa.Integer()),
                        sa.Column('num_processings', sa.Integer()),
                        sa.Column('new_contents', sa.Integer()),
                        sa.Column('queue_contents', sa.Integer()),
                        sa.Column("created_at", sa.DateTime, default=datetime.datetime.utcnow, nullable=False),
                        sa.Column("updated_at", sa.DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False),
                        sa.Column('others', JSON()),
                        schema=schema)
        op.create_primary_key('THROTTLER_PK', 'throttlers', ['throttler_id'], schema=schema)
        op.create_unique_constraint('THROTTLER_SITE_UQ', 'throttlers', ['site'], schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('requests', 'site', schema=schema)
        # op.drop_index("REQUESTS_STATUS_SITE", "requests", schema=schema)

        op.drop_column('transforms', 'site', schema=schema)
        # op.drop_index("TRANSFORMS_STATUS_SITE", "transforms", schema=schema)

        op.drop_column('processings', 'site', schema=schema)
        # op.drop_index("PROCESSINGS_STATUS_SITE", "processings", schema=schema)

        op.drop_column('messages', 'fetching_id', schema=schema)

        op.drop_constraint('THROTTLER_SITE_UQ', table_name='throttlers', schema=schema)
        op.drop_constraint('THROTTLER_PK', table_name='throttlers', schema=schema)
        op.drop_table('throttlers', schema=schema)
        op.drop_sequence('THROTTLER_ID_SEQ', schema=schema)
