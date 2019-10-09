#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
Utils to create the database or destroy the database
"""

import traceback

from sqlalchemy.engine import reflection
from sqlalchemy.schema import DropTable, DropConstraint, ForeignKeyConstraint, MetaData, Table

from idds.orm.base import session, models


def build_database(echo=True, tests=False):
    """Build the database. """
    engine = session.get_engine(echo=echo)
    models.register_models(engine)


def destroy_database(echo=True):
    """Destroy the database"""
    engine = session.get_engine(echo=echo)
    models.unregister_models(engine)


def destory_everything(echo=True):
    """ Using metadata.reflect() to get all constraints and tables.
        metadata.drop_all() as it handles cyclical constraints between tables.
        Ref. http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DropEverything
    """
    engine = session.get_engine(echo=echo)
    conn = engine.connect()

    # the transaction only applies if the DB supports
    # transactional DDL, i.e. Postgresql, MS SQL Server
    trans = conn.begin()

    inspector = reflection.Inspector.from_engine(engine)

    # gather all data first before dropping anything.
    # some DBs lock after things have been dropped in
    # a transaction.
    metadata = MetaData()

    tbs = []
    all_fks = []

    for table_name in inspector.get_table_names():
        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            if not fk['name']:
                continue
            fks.append(ForeignKeyConstraint((), (), name=fk['name']))
        t = Table(table_name, metadata, *fks)
        tbs.append(t)
        all_fks.extend(fks)

    for fkc in all_fks:
        try:
            print(str(DropConstraint(fkc)) + ';')
            conn.execute(DropConstraint(fkc))
        except:  # noqa: B901
            print(traceback.format_exc())

    for table in tbs:
        try:
            print(str(DropTable(table)).strip() + ';')
            conn.execute(DropTable(table))
        except:  # noqa: B901
            print(traceback.format_exc())

    trans.commit()


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
