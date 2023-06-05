#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023

"""add poll period in message

Revision ID: 0204f391c32d
Revises: 53d0af715dab
Create Date: 2023-05-21 19:20:00.448768+00:00

"""

import datetime

from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0204f391c32d'
down_revision = '53d0af715dab'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.add_column('messages', sa.Column('poll_period', sa.Interval, default=datetime.timedelta(seconds=300)), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        op.drop_column('messages', 'poll_period', schema=schema)
