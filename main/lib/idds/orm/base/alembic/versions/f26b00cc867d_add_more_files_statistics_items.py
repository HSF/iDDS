#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""add more files statistics items

Revision ID: f26b00cc867d
Revises: 8c3a2b989a93
Create Date: 2025-06-05 08:32:07.454903+00:00

"""
from alembic import op
from alembic import context
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f26b00cc867d'
down_revision = '8c3a2b989a93'
branch_labels = None
depends_on = None


def upgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.add_column('collections', sa.Column('preprocessing_files', sa.BigInteger()), schema=schema)
        op.add_column('collections', sa.Column('activated_files', sa.BigInteger()), schema=schema)


def downgrade() -> None:
    if context.get_context().dialect.name in ['oracle', 'mysql', 'postgresql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''

        op.drop_column('collections', 'preprocessing_files', schema=schema)
        op.drop_column('collections', 'activated_files', schema=schema)
