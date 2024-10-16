#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024

"""add process thread locking information

Revision ID: a844dae57021
Revises: 3073c5de8f73
Create Date: 2024-10-15 13:55:49.485737+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a844dae57021'
down_revision = '3073c5de8f73'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('requests', sa.Column('locking_hostname', sa.String(50)), schema=schema)
        op.add_column('requests', sa.Column('locking_pid', sa.BigInteger()), schema=schema)
        op.add_column('requests', sa.Column('locking_thread_id', sa.BigInteger()), schema=schema)
        op.add_column('requests', sa.Column('locking_thread_name', sa.String(100)), schema=schema)

        op.add_column('transforms', sa.Column('locking_hostname', sa.String(50)), schema=schema)
        op.add_column('transforms', sa.Column('locking_pid', sa.BigInteger()), schema=schema)
        op.add_column('transforms', sa.Column('locking_thread_id', sa.BigInteger()), schema=schema)
        op.add_column('transforms', sa.Column('locking_thread_name', sa.String(100)), schema=schema)

        op.add_column('processings', sa.Column('locking_hostname', sa.String(50)), schema=schema)
        op.add_column('processings', sa.Column('locking_pid', sa.BigInteger()), schema=schema)
        op.add_column('processings', sa.Column('locking_thread_id', sa.BigInteger()), schema=schema)
        op.add_column('processings', sa.Column('locking_thread_name', sa.String(100)), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('requests', 'locking_hostname', schema=schema)
        op.drop_column('requests', 'locking_pid', schema=schema)
        op.drop_column('requests', 'locking_thread_id', schema=schema)
        op.drop_column('requests', 'locking_thread_name', schema=schema)

        op.drop_column('transforms', 'locking_hostname', schema=schema)
        op.drop_column('transforms', 'locking_pid', schema=schema)
        op.drop_column('transforms', 'locking_thread_id', schema=schema)
        op.drop_column('transforms', 'locking_thread_name', schema=schema)

        op.drop_column('processings', 'locking_hostname', schema=schema)
        op.drop_column('processings', 'locking_pid', schema=schema)
        op.drop_column('processings', 'locking_thread_id', schema=schema)
        op.drop_column('processings', 'locking_thread_name', schema=schema)
