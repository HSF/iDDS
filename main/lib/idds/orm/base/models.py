#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2020


"""
SQLAlchemy models for idds relational data
"""

import datetime
from enum import Enum

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, String, event, DDL
from sqlalchemy.ext.compiler import compiles
# from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_mapper
from sqlalchemy.schema import CheckConstraint, UniqueConstraint, Index, PrimaryKeyConstraint, ForeignKeyConstraint, Sequence, Table

from idds.common.constants import (RequestType, RequestStatus, RequestLocking,
                                   WorkprogressStatus, WorkprogressLocking,
                                   TransformType, TransformStatus, TransformLocking,
                                   ProcessingStatus, ProcessingLocking,
                                   CollectionStatus, CollectionLocking, CollectionType,
                                   CollectionRelationType, ContentType, ContentRelationType,
                                   ContentStatus, ContentLocking, GranularityType,
                                   MessageType, MessageStatus, MessageLocking,
                                   MessageSource, MessageDestination)
from idds.common.utils import date_to_str
from idds.orm.base.enum import EnumSymbol
from idds.orm.base.types import JSON, JSONString, EnumWithValue
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
        attr_items = list(self.__dict__.items())
        items_extend = self._items_extend()
        return attr_items + items_extend

    def _items_extend(self):
        return []

    def to_dict(self):
        return {key: value for key, value
                in self.items() if not key.startswith('_')}

    def to_dict_json(self):
        return {key: self._expand_item(value) for key, value
                in self.items() if not key.startswith('_')}

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
        elif isinstance(obj, Enum):
            return obj.value

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
    substatus = Column(EnumWithValue(RequestStatus), default=0)
    locking = Column(EnumWithValue(RequestLocking))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    errors = Column(JSON())
    _request_metadata = Column('request_metadata', JSON())
    _processing_metadata = Column('processing_metadata', JSON())

    @property
    def request_metadata(self):
        if self._request_metadata and 'workflow' in self._request_metadata:
            workflow = self._request_metadata['workflow']
            workflow_data = None
            if self._processing_metadata and 'workflow_data' in self._processing_metadata:
                workflow_data = self._processing_metadata['workflow_data']
            if workflow is not None and workflow_data is not None:
                workflow.metadata = workflow_data
                self._request_metadata['workflow'] = workflow
        return self._request_metadata

    @request_metadata.setter
    def request_metadata(self, request_metadata):
        if self._request_metadata is None:
            self._request_metadata = request_metadata
        if self._processing_metadata is None:
            self._processing_metadata = {}
        if request_metadata and 'workflow' in request_metadata:
            workflow = request_metadata['workflow']
            self._processing_metadata['workflow_data'] = workflow.metadata

    @property
    def processing_metadata(self):
        return self._processing_metadata

    @processing_metadata.setter
    def processing_metadata(self, processing_metadata):
        if self._processing_metadata is None:
            self._processing_metadata = {}
        if processing_metadata:
            for k in processing_metadata:
                if k != 'workflow_data':
                    self._processing_metadata[k] = processing_metadata[k]

    def _items_extend(self):
        return [('request_metadata', self.request_metadata),
                ('processing_metadata', self.processing_metadata)]

    def update(self, values, flush=True, session=None):
        if values and 'request_metadata' in values and 'workflow' in values['request_metadata']:
            workflow = values['request_metadata']['workflow']

            if workflow is not None:
                if 'processing_metadata' not in values:
                    values['processing_metadata'] = {}
                values['processing_metadata']['workflow_data'] = workflow.metadata
        if values and 'request_metadata' in values:
            del values['request_metadata']
        if values and 'processing_metadata' in values:
            values['_processing_metadata'] = values['processing_metadata']
            del values['processing_metadata']
        super(Request, self).update(values, flush, session)

    _table_args = (PrimaryKeyConstraint('request_id', name='REQUESTS_PK'),
                   CheckConstraint('status IS NOT NULL', name='REQUESTS_STATUS_ID_NN'),
                   # UniqueConstraint('name', 'scope', 'requester', 'request_type', 'transform_tag', 'workload_id', name='REQUESTS_NAME_SCOPE_UQ '),
                   Index('REQUESTS_SCOPE_NAME_IDX', 'workload_id', 'request_id', 'name', 'scope'),
                   Index('REQUESTS_STATUS_PRIO_IDX', 'status', 'priority', 'workload_id', 'request_id', 'locking', 'updated_at', 'next_poll_at', 'created_at'))


class Workprogress(BASE, ModelBase):
    """Represents a workprogress which monitors the progress of a workflow"""
    __tablename__ = 'workprogresses'
    workprogress_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('WORKPROGRESS_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    # requester = Column(String(20))
    # request_type = Column(EnumWithValue(RequestType))
    # transform_tag = Column(String(20))
    # workload_id = Column(Integer())
    priority = Column(Integer())
    status = Column(EnumWithValue(WorkprogressStatus))
    substatus = Column(EnumWithValue(WorkprogressStatus), default=0)
    locking = Column(EnumWithValue(WorkprogressLocking))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    errors = Column(JSON())
    workprogress_metadata = Column(JSON())
    processing_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('workprogress_id', name='WORKPROGRESS_PK'),
                   ForeignKeyConstraint(['request_id'], ['requests.request_id'], name='REQ2WORKPROGRESS_REQ_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='WORKPROGRESS_STATUS_ID_NN'),
                   # UniqueConstraint('name', 'scope', 'requester', 'request_type', 'transform_tag', 'workload_id', name='REQUESTS_NAME_SCOPE_UQ '),
                   Index('WORKPROGRESS_SCOPE_NAME_IDX', 'workprogress_id', 'request_id', 'name', 'scope'),
                   Index('WORKPROGRESS_STATUS_PRIO_IDX', 'status', 'priority', 'workprogress_id', 'locking', 'updated_at', 'next_poll_at', 'created_at'))


class Transform(BASE, ModelBase):
    """Represents a transform"""
    __tablename__ = 'transforms'
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('TRANSFORM_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    transform_type = Column(EnumWithValue(TransformType))
    transform_tag = Column(String(20))
    priority = Column(Integer())
    safe2get_output_from_input = Column(Integer())
    status = Column(EnumWithValue(TransformStatus))
    substatus = Column(EnumWithValue(TransformStatus), default=0)
    locking = Column(EnumWithValue(TransformLocking))
    retries = Column(Integer(), default=0)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    started_at = Column("started_at", DateTime)
    finished_at = Column("finished_at", DateTime)
    expired_at = Column("expired_at", DateTime)
    _transform_metadata = Column('transform_metadata', JSON())
    _running_metadata = Column('running_metadata', JSON())

    @property
    def transform_metadata(self):
        if self._transform_metadata and 'work' in self._transform_metadata:
            work = self._transform_metadata['work']
            work_data = None
            if self._running_metadata and 'work_data' in self._running_metadata:
                work_data = self._running_metadata['work_data']
            if work is not None and work_data is not None:
                work.metadata = work_data
                self._transform_metadata['work'] = work
        return self._transform_metadata

    @transform_metadata.setter
    def transform_metadata(self, transform_metadata):
        if self._transform_metadata is None:
            self._transform_metadata = transform_metadata
        if self._running_metadata is None:
            self._running_metadata = {}
        if transform_metadata and 'work' in transform_metadata:
            work = transform_metadata['work']
            self._running_metadata['work_data'] = work.metadata

    @property
    def running_metadata(self):
        return self._running_metadata

    @running_metadata.setter
    def running_metadata(self, running_metadata):
        if self._running_metadata is None:
            self._running_metadata = {}
        if running_metadata:
            for k in running_metadata:
                if k != 'work_data':
                    self._running_metadata[k] = running_metadata[k]

    def _items_extend(self):
        return [('transform_metadata', self.transform_metadata),
                ('running_metadata', self.running_metadata)]

    def update(self, values, flush=True, session=None):
        if values and 'transform_metadata' in values and 'work' in values['transform_metadata']:
            work = values['transform_metadata']['work']
            if work is not None:
                if 'running_metadata' not in values:
                    values['running_metadata'] = {}
                values['running_metadata']['work_data'] = work.metadata
        if values and 'transform_metadata' in values:
            del values['transform_metadata']
        if values and 'running_metadata' in values:
            values['_running_metadata'] = values['running_metadata']
            del values['running_metadata']
        super(Transform, self).update(values, flush, session)

    _table_args = (PrimaryKeyConstraint('transform_id', name='TRANSFORMS_PK'),
                   CheckConstraint('status IS NOT NULL', name='TRANSFORMS_STATUS_ID_NN'),
                   Index('TRANSFORMS_TYPE_TAG_IDX', 'transform_type', 'transform_tag', 'transform_id'),
                   Index('TRANSFORMS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'next_poll_at', 'created_at'))


class Workprogress2transform(BASE, ModelBase):
    """Represents a workprogress to transform"""
    __tablename__ = 'wp2transforms'
    workprogress_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)

    _table_args = (PrimaryKeyConstraint('workprogress_id', 'transform_id', name='WP2TRANSFORM_PK'),
                   ForeignKeyConstraint(['workprogress_id'], ['workprogresses.workprogress_id'], name='WP2TRANSFORM_WORK_ID_FK'),
                   ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='WP2TRANSFORM_TRANS_ID_FK'))


class Processing(BASE, ModelBase):
    """Represents a processing"""
    __tablename__ = 'processings'
    processing_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('PROCESSING_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    status = Column(EnumWithValue(ProcessingStatus))
    substatus = Column(EnumWithValue(ProcessingStatus), default=0)
    locking = Column(EnumWithValue(ProcessingLocking))
    submitter = Column(String(20))
    submitted_id = Column(Integer())
    granularity = Column(Integer())
    granularity_type = Column(EnumWithValue(GranularityType))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    submitted_at = Column("submitted_at", DateTime)
    finished_at = Column("finished_at", DateTime)
    expired_at = Column("expired_at", DateTime)
    _processing_metadata = Column('processing_metadata', JSON())
    _running_metadata = Column('running_metadata', JSON())
    output_metadata = Column(JSON())

    @property
    def processing_metadata(self):
        if self._processing_metadata and 'processing' in self._processing_metadata:
            proc = self._processing_metadata['processing']
            proc_data = None
            if self._running_metadata and 'processing_data' in self._running_metadata:
                proc_data = self._running_metadata['processing_data']
            if proc is not None and proc_data is not None:
                proc.metadata = proc_data
                self._processing_metadata['processing'] = proc
        return self._processing_metadata

    @processing_metadata.setter
    def processing_metadata(self, processing_metadata):
        if self._processing_metadata is None:
            self._processing_metadata = processing_metadata
        if self._running_metadata is None:
            self._running_metadata = {}
        if processing_metadata and 'processing' in processing_metadata:
            proc = processing_metadata['processing']
            self._running_metadata['processing_data'] = proc.metadata

    @property
    def running_metadata(self):
        return self._running_metadata

    @running_metadata.setter
    def running_metadata(self, running_metadata):
        if self._running_metadata is None:
            self._running_metadata = {}
        if running_metadata:
            for k in running_metadata:
                if k != 'processing_data':
                    self._running_metadata[k] = running_metadata[k]

    def _items_extend(self):
        return [('processing_metadata', self.processing_metadata),
                ('running_metadata', self.running_metadata)]

    def update(self, values, flush=True, session=None):
        if values and 'processing_metadata' in values and 'processing' in values['processing_metadata']:
            proc = values['processing_metadata']['processing']
            if proc is not None:
                if 'running_metadata' not in values:
                    values['running_metadata'] = {}
                values['running_metadata']['processing_data'] = proc.metadata
        if values and 'processing_metadata' in values:
            del values['processing_metadata']
        if values and 'running_metadata' in values:
            values['_running_metadata'] = values['running_metadata']
            del values['running_metadata']
        super(Transform, self).update(values, flush, session)

    _table_args = (PrimaryKeyConstraint('processing_id', name='PROCESSINGS_PK'),
                   ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='PROCESSINGS_TRANSFORM_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='PROCESSINGS_STATUS_ID_NN'),
                   CheckConstraint('transform_id IS NOT NULL', name='PROCESSINGS_TRANSFORM_ID_NN'),
                   Index('PROCESSINGS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'next_poll_at', 'created_at'))


class Collection(BASE, ModelBase):
    """Represents a collection"""
    __tablename__ = 'collections'
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('COLLECTION_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    coll_type = Column(EnumWithValue(CollectionType))
    relation_type = Column(EnumWithValue(CollectionRelationType))
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    bytes = Column(Integer())
    status = Column(EnumWithValue(CollectionStatus))
    substatus = Column(EnumWithValue(CollectionStatus), default=0)
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
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
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
                   Index('COLLECTIONS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'next_poll_at', 'created_at'))


class Content(BASE, ModelBase):
    """Represents a content"""
    __tablename__ = 'contents'
    content_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('CONTENT_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    map_id = Column(BigInteger().with_variant(Integer, "sqlite"), default=0)
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    min_id = Column(Integer())
    max_id = Column(Integer())
    content_type = Column(EnumWithValue(ContentType))
    content_relation_type = Column(EnumWithValue(ContentRelationType))
    status = Column(EnumWithValue(ContentStatus))
    substatus = Column(EnumWithValue(ContentStatus))
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
    content_metadata = Column(JSONString())

    _table_args = (PrimaryKeyConstraint('content_id', name='CONTENTS_PK'),
                   # UniqueConstraint('name', 'scope', 'coll_id', 'content_type', 'min_id', 'max_id', name='CONTENT_SCOPE_NAME_UQ'),
                   # UniqueConstraint('name', 'scope', 'coll_id', 'min_id', 'max_id', name='CONTENT_SCOPE_NAME_UQ'),
                   # UniqueConstraint('content_id', 'coll_id', name='CONTENTS_UQ'),
                   UniqueConstraint('transform_id', 'coll_id', 'map_id', 'name', name='CONTENT_ID_UQ'),
                   ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='CONTENTS_TRANSFORM_ID_FK'),
                   ForeignKeyConstraint(['coll_id'], ['collections.coll_id'], name='CONTENTS_COLL_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='CONTENTS_STATUS_ID_NN'),
                   CheckConstraint('coll_id IS NOT NULL', name='CONTENTS_COLL_ID_NN'),
                   Index('CONTENTS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'created_at'),
                   Index('CONTENTS_ID_NAME_IDX', 'coll_id', 'scope', 'name', 'status'),
                   Index('CONTENTS_REQ_TF_COLL_IDX', 'request_id', 'transform_id', 'coll_id', 'status'))


class Health(BASE, ModelBase):
    """Represents the status of the running agents"""
    __tablename__ = 'health'
    health_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                       Sequence('HEALTH_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                       primary_key=True)
    agent = Column(String(30))
    hostname = Column(String(127))
    pid = Column(Integer, autoincrement=False)
    thread_id = Column(BigInteger, autoincrement=False)
    thread_name = Column(String(255))
    payload = Column(String(255))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    _table_args = (PrimaryKeyConstraint('health_id', name='HEALTH_PK'),
                   UniqueConstraint('agent', 'hostname', 'pid', 'thread_id', name='HEALTH_UK'))


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
    destination = Column(EnumWithValue(MessageDestination))
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    transform_id = Column(Integer())
    processing_id = Column(Integer())
    num_contents = Column(Integer())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    msg_content = Column(JSON())

    _table_args = (PrimaryKeyConstraint('msg_id', name='MESSAGES_PK'))


def register_models(engine):
    """
    Creates database tables for all models with the given engine
    """

    models = (Request, Workprogress, Transform, Workprogress2transform, Processing, Collection, Content, Health, Message)

    for model in models:
        model.metadata.create_all(engine)   # pylint: disable=maybe-no-member


def unregister_models(engine):
    """
    Drops database tables for all models with the given engine
    """

    models = (Request, Workprogress, Transform, Workprogress2transform, Processing, Collection, Content, Health, Message)

    for model in models:
        model.metadata.drop_all(engine)   # pylint: disable=maybe-no-member
