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
data types

Borrowed from:
https://github.com/rucio/rucio/blob/master/lib/rucio/db/sqla/types.py
"""

import uuid

from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.dialects.oracle import RAW, CLOB
from sqlalchemy.dialects.mysql import BINARY
from sqlalchemy.types import TypeDecorator, CHAR, String, Integer
import sqlalchemy.types as types

from idds.common.utils import json_dumps, json_loads


class GUID(TypeDecorator):
    """
    Platform-independent GUID type.
    Uses PostgreSQL's UUID type,
    uses Oracle's RAW type,
    uses MySQL's BINARY type,
    otherwise uses CHAR(32), storing as stringified hex values.
    """
    impl = CHAR

    cache_ok = True

    def generate_uuid(self):
        return str(uuid.uuid4()).replace('-', '').lower()

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        elif dialect.name == 'oracle':
            return dialect.type_descriptor(RAW(16))
        elif dialect.name == 'mysql':
            return dialect.type_descriptor(BINARY(16))
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value).lower()
        elif dialect.name == 'oracle':
            return uuid.UUID(value).bytes
        elif dialect.name == 'mysql':
            return uuid.UUID(value).bytes
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value)
            else:
                # hexstring
                return "%.32x" % value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'oracle':
            return str(uuid.UUID(bytes=value)).replace('-', '').lower()
        elif dialect.name == 'mysql':
            return str(uuid.UUID(bytes=value)).replace('-', '').lower()
        else:
            return str(uuid.UUID(value)).replace('-', '').lower()


class JSON(TypeDecorator):
    """
    Platform independent json type
    JSONB for postgres , JSON for the rest
    """

    impl = types.JSON

    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(JSONB())
        elif dialect.name == 'mysql':
            return dialect.type_descriptor(String(40960))
            # return dialect.type_descriptor(types.JSON())
        elif dialect.name == 'oracle':
            return dialect.type_descriptor(CLOB())
        else:
            return dialect.type_descriptor(String())

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return json_dumps(value)
        elif dialect.name == 'oracle':
            return json_dumps(value)
        elif dialect.name == 'mysql':
            return json_dumps(value)
        else:
            return json_dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'oracle':
            return json_loads(value)
        elif dialect.name == 'mysql':
            return json_loads(value)
        else:
            return json_loads(value)


class JSONString(TypeDecorator):
    """
    Platform independent json type
    JSONB for postgres , JSON for the rest
    """

    impl = types.JSON

    cache_ok = True

    def __init__(self, length=1024, *args, **kwargs):
        super(JSONString, self).__init__(*args, **kwargs)
        self._length = length

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(String(self._length))
        elif dialect.name == 'mysql':
            return dialect.type_descriptor(String(self._length))
            # return dialect.type_descriptor(types.JSON())
        elif dialect.name == 'oracle':
            return dialect.type_descriptor(String(self._length))
        else:
            return dialect.type_descriptor(String(self._length))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return json_dumps(value)
        elif dialect.name == 'oracle':
            return json_dumps(value)
        elif dialect.name == 'mysql':
            return json_dumps(value)
        else:
            return json_dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'oracle':
            return json_loads(value)
        elif dialect.name == 'mysql':
            return json_loads(value)
        else:
            return json_loads(value)


class EnumWithValue(TypeDecorator):
    """
    Enables passing in a Python enum and storing the enum's *value* in the db.
    The default would have stored the enum's *name* (ie the string).
    """
    impl = Integer
    cache_ok = True

    def __init__(self, enumtype, *args, **kwargs):
        super(EnumWithValue, self).__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value, dialect):
        if isinstance(value, int):
            return value

        return value.value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return self._enumtype(value)
