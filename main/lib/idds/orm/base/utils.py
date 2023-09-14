#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2023


"""
Utils to create the database or destroy the database
"""

import io
import os
import traceback
from typing import Union

from alembic.config import Config
from alembic import command

import sqlalchemy
# from sqlalchemy.engine import reflection
from sqlalchemy.engine import Inspector
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql.base import PGInspector
from sqlalchemy.schema import CreateSchema, MetaData, Table, DropTable, ForeignKeyConstraint, DropConstraint
from sqlalchemy.sql.ddl import DropSchema

from idds.common.config import config_has_option, config_get
from idds.orm.base import session, models


def build_database(echo=True, tests=False):
    """Build the database. """
    engine = session.get_engine(echo=echo)

    if config_has_option('database', 'schema'):
        schema = config_get('database', 'schema')
        if schema and not engine.dialect.has_schema(engine, schema):
            print('Schema set in config, trying to create schema:', schema)
            try:
                engine.execute(CreateSchema(schema))
            except Exception as e:
                print('Cannot create schema, please validate manually if schema creation is needed, continuing:', e)
                print(traceback.format_exc())

    models.register_models(engine)

    # record the head version of alembic
    alembic_cfg_file = os.environ.get("ALEMBIC_CONFIG")
    alembic_cfg = Config(alembic_cfg_file)

    output_buffer = io.StringIO()
    alembic_cfg1 = Config(alembic_cfg_file, stdout=output_buffer)
    current_version = command.current(alembic_cfg1)
    current_version = output_buffer.getvalue()
    print("current_version: " + current_version)

    if current_version is None or len(current_version) == 0:
        print("no current version, stamp it")
        command.stamp(alembic_cfg, "head")
        command.upgrade(alembic_cfg, "head")
    else:
        print("has current version, try to upgrade")
        command.upgrade(alembic_cfg, "head")


def destroy_database(echo=True):
    """Destroy the database"""
    engine = session.get_engine(echo=echo)

    try:
        models.unregister_models(engine)
    except Exception as e:
        print('Cannot destroy schema -- assuming already gone, continuing:', e)
        print(traceback.format_exc())


def destroy_everything(echo=True):
    """ Using metadata.reflect() to get all constraints and tables.
        metadata.drop_all() as it handles cyclical constraints between tables.
        Ref. http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DropEverything
    """
    engine = session.get_engine(echo=echo)

    try:
        # the transaction only applies if the DB supports
        # transactional DDL, i.e. Postgresql, MS SQL Server
        with engine.begin() as conn:

            inspector = inspect(conn)  # type: Union[Inspector, PGInspector]

            for tname, fkcs in reversed(
                    inspector.get_sorted_table_and_fkc_names(schema='*')):
                if tname:
                    drop_table_stmt = DropTable(Table(tname, MetaData(), schema='*'))
                    conn.execute(drop_table_stmt)
                elif fkcs:
                    if not engine.dialect.supports_alter:
                        continue
                    for tname, fkc in fkcs:
                        fk_constraint = ForeignKeyConstraint((), (), name=fkc)
                        Table(tname, MetaData(), fk_constraint)
                        drop_constraint_stmt = DropConstraint(fk_constraint)
                        conn.execute(drop_constraint_stmt)

            if config_has_option('database', 'schema'):
                schema = config_get('database', 'schema')
                if schema:
                    conn.execute(DropSchema(schema, cascade=True))

            if engine.dialect.name == 'postgresql':
                assert isinstance(inspector, PGInspector), 'expected a PGInspector'
                for enum in inspector.get_enums(schema='*'):
                    sqlalchemy.Enum(**enum).drop(bind=conn)

    except Exception as e:
        print('Cannot destroy db:', e)
        print(traceback.format_exc())


def dump_schema():
    """ Creates a schema dump to a specific database. """
    engine = session.get_dump_engine()
    models.register_models(engine)


def row2dict(row):
    """ Convert rows to dict. """
    row_dict = {}
    for col in row.keys():
        row_dict[str(col)] = row[col]
    return row_dict


def rows2dict(rows):
    """ Convert rows to dict. """
    results = []
    for row in rows:
        row_dict = {}
        for col in row.keys():
            row_dict[str(col)] = row[col]
        results.append(row_dict)
    return results
