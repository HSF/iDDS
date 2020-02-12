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
SQLAlchemy models for idds relational data
"""

import datetime

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, String, event, DDL
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import object_mapper
from sqlalchemy.schema import CheckConstraint, UniqueConstraint, Index, PrimaryKeyConstraint, Sequence, Table

from idds.common.constants import (RequestType, RequestStatus, RequestSubStatus,
                                   MessageType, MessageStatus, MessageSource)
from idds.common.utils import date_to_str
from idds.orm.base.enum import EnumSymbol
from idds.orm.base.types import JSON, EnumWithValue
from idds.orm.base.session import BASE, DEFAULT_SCHEMA_NAME
from idds.common.constants import (SCOPE_LENGTH, NAME_LENGTH)


@compiles(Boolean, "oracle")
def compile_binary_oracle(type_, compiler, **kw):
    return "NUMBER(1)"


@event.listens_for(Table, "after_create")
def _psql_autoincrement(target, connection, **kw):
    if connection.dialect.name == 'mysql' and target.name == 'ess_coll':
        DDL("alter table ess_coll modify coll_id bigint(20) not null unique auto_increment")


class ModelBase(object):
    """Base class for IDDS Models"""

    def save(self, flush=True, session=None):
        """Save this object"""
        session.add(self)
        if flush:
            session.flush()

    def delete(self, flush=True, session=None):
        """Delete this object"""
        session.delete(self)
        if flush:
            session.flush()

    def update(self, values, flush=True, session=None):
        """dict.update() behaviour."""
        for k, v in values.iteritems():
            self[k] = v
        self["updated_at"] = datetime.datetime.utcnow()
        if session and flush:
            session.flush()

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        self._i = iter(object_mapper(self).columns)
        return self

    def next(self):
        n = self._i.next().name
        return n, getattr(self, n)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def to_dict(self):
        return {key: self._expand_item(value) for key, value
                in self.__dict__.items() if not key.startswith('_')}

    @classmethod
    def _expand_item(cls, obj):
        """
        Return a valid representation of `obj` depending on its type.
        """
        if isinstance(obj, datetime.datetime):
            return date_to_str(obj)
        elif isinstance(obj, (datetime.time, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return obj.days * 24 * 60 * 60 + obj.seconds
        elif isinstance(obj, EnumSymbol):
            return obj.description

        return obj


class Request(BASE, ModelBase):
    """Represents a pre-cache request from other service"""
    __tablename__ = 'requests'
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('REQUEST_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    requester = Column(String(20))
    request_type = Column(EnumWithValue(RequestType))
    transform_tag = Column(String(10))
    priority = Column(Integer())
    status = Column(EnumWithValue(RequestStatus))
    substatus = Column(EnumWithValue(RequestSubStatus))
    workload_id = Column(Integer())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    errors = Column(JSON())
    request_metadata = Column(JSON())
    processing_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('request_id', name='_REQUESTS_PK'),
                   CheckConstraint('status IS NOT NULL', name='REQ_STATUS_ID_NN'),
                   UniqueConstraint('name', 'scope', 'requester', 'request_type', 'transform_tag', 'workload_id', name='REQUESTS_NAME_SCOPE_UQ '),
                   Index('REQUESTS_SCOPE_NAME_IDX', 'scope', 'name', 'workload_id'),
                   Index('REQUESTS_STATUS_PRIO_IDX', 'status', 'priority', 'request_id'))


class Message(BASE, ModelBase):
    """Represents the event messages"""
    __tablename__ = 'messages'
    msg_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                    Sequence('MESSAGE_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                    primary_key=True)
    msg_type = Column(EnumWithValue(MessageType))
    status = Column(EnumWithValue(MessageStatus))
    source = Column(EnumWithValue(MessageSource))
    transform_id = Column(Integer())
    num_contents = Column(Integer())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    msg_content = Column(JSON())

    _table_args = (PrimaryKeyConstraint('msg_id', name='MESSAGES_PK'))


def register_models(engine):
    """
    Creates database tables for all models with the given engine
    """

    models = (Request)

    for model in models:
        model.metadata.create_all(engine)   # pylint: disable=maybe-no-member


def unregister_models(engine):
    """
    Drops database tables for all models with the given engine
    """

    models = (Request)

    for model in models:
        model.metadata.drop_all(engine)   # pylint: disable=maybe-no-member
