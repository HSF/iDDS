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
from sqlalchemy.schema import CheckConstraint, UniqueConstraint, Index, PrimaryKeyConstraint, ForeignKeyConstraint, Sequence, Table

from idds.common.constants import (RequestType, RequestStatus, RequestLocking,
                                   TransformType, TransformStatus, TransformLocking,
                                   ProcessingStatus, ProcessingLocking,
                                   CollectionStatus, CollectionLocking,
                                   ContentStatus, ContentLocking,
                                   MessageType, MessageStatus, MessageLocking, MessageSource)
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
    transform_tag = Column(String(20))
    workload_id = Column(Integer())
    priority = Column(Integer())
    status = Column(EnumWithValue(RequestStatus))
    substatus = Column(Integer())
    locking = Column(EnumWithValue(RequestLocking))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    errors = Column(JSON())
    request_metadata = Column(JSON())
    processing_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('request_id', name='REQUESTS_PK'),
                   CheckConstraint('status IS NOT NULL', name='REQUESTS_STATUS_ID_NN'),
                   # UniqueConstraint('name', 'scope', 'requester', 'request_type', 'transform_tag', 'workload_id', name='REQUESTS_NAME_SCOPE_UQ '),
                   Index('REQUESTS_SCOPE_NAME_IDX', 'workload_id', 'request_id', 'name', 'scope'),
                   Index('REQUESTS_STATUS_PRIO_IDX', 'status', 'priority', 'workload_id', 'request_id', 'locking', 'updated_at', 'created_at'))


class Transform(BASE, ModelBase):
    """Represents a transform"""
    __tablename__ = 'transforms'
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('TRANSFORM_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    transform_type = Column(EnumWithValue(TransformType))
    transform_tag = Column(String(20))
    priority = Column(Integer())
    safe2get_output_from_input = Column(Integer())
    status = Column(EnumWithValue(TransformStatus))
    substatus = Column(Integer())
    locking = Column(EnumWithValue(TransformLocking))
    retries = Column(Integer(), default=0)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    started_at = Column("started_at", DateTime)
    finished_at = Column("finished_at", DateTime)
    expired_at = Column("expired_at", DateTime)
    transform_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('transform_id', name='TRANSFORMS_PK'),
                   CheckConstraint('status IS NOT NULL', name='TRANSFORMS_STATUS_ID_NN'),
                   Index('TRANSFORMS_TYPE_TAG_IDX', 'transform_type', 'transform_tag', 'transform_id'),
                   Index('TRANSFORMS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'created_at'))


class Req2transform(BASE, ModelBase):
    """Represents a request to transform"""
    __tablename__ = 'req2transforms'
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))

    _table_args = (PrimaryKeyConstraint('request_id', 'transform_id', name='REQ2TRANSFORMS_PK'),
                   ForeignKeyConstraint(['request_id'], ['requests.request_id'], name='REQ2TRANSFORMS_REQ_ID_FK'),
                   ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='REQ2TRANSFORMS_TRANS_ID_FK'))


class Processing(BASE, ModelBase):
    """Represents a processing"""
    __tablename__ = 'processings'
    processing_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('PROCESSING_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    status = Column(EnumWithValue(ProcessingStatus))
    substatus = Column(Integer())
    locking = Column(EnumWithValue(ProcessingLocking))
    submitter = Column(String(20))
    submitted_id = Column(Integer())
    granularity = Column(Integer())
    granularity_type = Column(Integer())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    submitted_at = Column("started_at", DateTime)
    finished_at = Column("finished_at", DateTime)
    expired_at = Column("expired_at", DateTime)
    processing_metadata = Column(JSON())
    output_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('processing_id', name='PROCESSINGS_PK'),
                   ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='PROCESSINGS_TRANSFORM_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='PROCESSINGS_STATUS_ID_NN'),
                   CheckConstraint('transform_id IS NOT NULL', name='PROCESSINGS_TRANSFORM_ID_NN'),
                   Index('PROCESSINGS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'created_at'))


class Collection(BASE, ModelBase):
    """Represents a collection"""
    __tablename__ = 'collections'
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('COLLECTION_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    coll_type = Column(Integer())
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    relation_type = Column(Integer())
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    bytes = Column(Integer())
    status = Column(EnumWithValue(CollectionStatus))
    substatus = Column(Integer())
    locking = Column(EnumWithValue(CollectionLocking))
    total_files = Column(Integer())
    storage_id = Column(Integer())
    new_files = Column(Integer())
    processed_files = Column(Integer())
    processing_files = Column(Integer())
    processing_id = Column(Integer())
    retries = Column(Integer(), default=0)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    coll_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('coll_id', name='COLLECTIONS_PK'),
                   UniqueConstraint('name', 'scope', 'transform_id', 'relation_type', name='COLLECTIONS_NAME_SCOPE_UQ'),
                   ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='COLLECTIONS_TRANSFORM_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='COLLECTIONS_STATUS_ID_NN'),
                   CheckConstraint('transform_id IS NOT NULL', name='COLLECTIONS_TRANSFORM_ID_NN'),
                   Index('COLLECTIONS_STATUS_RELAT_IDX', 'status', 'relation_type'),
                   Index('COLLECTIONS_TRANSFORM_IDX', 'transform_id', 'coll_id'),
                   Index('COLLECTIONS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'created_at'))


class Content(BASE, ModelBase):
    """Represents a content"""
    __tablename__ = 'contents'
    content_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('CONTENT_ID_SEQ', schema=DEFAULT_SCHEMA_NAME))
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    min_id = Column(Integer())
    max_id = Column(Integer())
    content_type = Column(Integer())
    status = Column(EnumWithValue(ContentStatus))
    substatus = Column(Integer())
    locking = Column(EnumWithValue(ContentLocking))
    bytes = Column(Integer())
    md5 = Column(String(32))
    adler32 = Column(String(8))
    processing_id = Column(Integer())
    storage_id = Column(Integer())
    retries = Column(Integer(), default=0)
    path = Column(String(4000))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    content_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('name', 'scope', 'coll_id', 'content_type', 'min_id', 'max_id', name='CONTENTS_PK'),
                   UniqueConstraint('content_id', 'coll_id', name='CONTENTS_UQ'),
                   ForeignKeyConstraint(['coll_id'], ['collections.coll_id'], name='CONTENTS_COLL_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='CONTENTS_STATUS_ID_NN'),
                   CheckConstraint('coll_id IS NOT NULL', name='CONTENTS_COLL_ID_NN'),
                   Index('CONTENTS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'created_at'))


class Message(BASE, ModelBase):
    """Represents the event messages"""
    __tablename__ = 'messages'
    msg_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                    Sequence('MESSAGE_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                    primary_key=True)
    msg_type = Column(EnumWithValue(MessageType))
    status = Column(EnumWithValue(MessageStatus))
    substatus = Column(Integer())
    locking = Column(EnumWithValue(MessageLocking))
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

    models = (Request, Transform, Req2transform, Processing, Collection, Content)

    for model in models:
        model.metadata.create_all(engine)   # pylint: disable=maybe-no-member


def unregister_models(engine):
    """
    Drops database tables for all models with the given engine
    """

    models = (Request, Transform, Req2transform, Processing, Collection, Content)

    for model in models:
        model.metadata.drop_all(engine)   # pylint: disable=maybe-no-member
