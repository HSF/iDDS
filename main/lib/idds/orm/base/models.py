#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024


"""
SQLAlchemy models for idds relational data
"""

import datetime
from enum import Enum

from sqlalchemy import func
from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, String, Float, event, DDL, Interval
from sqlalchemy.ext.compiler import compiles
# from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_mapper
from sqlalchemy.schema import CheckConstraint, UniqueConstraint, Index, PrimaryKeyConstraint, ForeignKeyConstraint, Sequence, Table

from idds.common.constants import (RequestType, RequestStatus, RequestLocking,
                                   WorkprogressStatus, WorkprogressLocking,
                                   TransformType, TransformStatus, TransformLocking,
                                   ProcessingType, ProcessingStatus, ProcessingLocking,
                                   CollectionStatus, CollectionLocking, CollectionType,
                                   CollectionRelationType, ContentType, ContentRelationType,
                                   ContentStatus, ContentFetchStatus, ContentLocking, GranularityType,
                                   MessageType, MessageStatus, MessageLocking,
                                   MessageSource, MessageDestination, ThrottlerStatus,
                                   CommandType, CommandStatus, CommandLocking,
                                   CommandLocation, HealthStatus, MetaStatus)
from idds.common.event import (EventType, EventStatus)
from idds.common.utils import date_to_str
from idds.orm.base.enum import EnumSymbol
from idds.orm.base.types import JSON, JSONString, EnumWithValue
from idds.orm.base.session import BASE, DEFAULT_SCHEMA_NAME
from idds.common.constants import (SCOPE_LENGTH, NAME_LENGTH, LONG_NAME_LENGTH)


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
    request_type = Column(EnumWithValue(RequestType), nullable=False)
    username = Column(String(20))
    userdn = Column(String(200))
    transform_tag = Column(String(20))
    workload_id = Column(Integer())
    priority = Column(Integer())
    status = Column(EnumWithValue(RequestStatus), nullable=False)
    substatus = Column(EnumWithValue(RequestStatus), default=0)
    oldstatus = Column(EnumWithValue(RequestStatus), default=0)
    locking = Column(EnumWithValue(RequestLocking), nullable=False)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    new_retries = Column(Integer(), default=0)
    update_retries = Column(Integer(), default=0)
    max_new_retries = Column(Integer(), default=3)
    max_update_retries = Column(Integer(), default=0)
    new_poll_period = Column(Interval(), default=datetime.timedelta(seconds=1))
    update_poll_period = Column(Interval(), default=datetime.timedelta(seconds=10))
    site = Column(String(50))
    errors = Column(JSONString(1024))
    _request_metadata = Column('request_metadata', JSON())
    _processing_metadata = Column('processing_metadata', JSON())

    @property
    def request_metadata(self):
        if self._request_metadata:
            if 'workflow' in self._request_metadata:
                workflow = self._request_metadata['workflow']
                workflow_data = None
                if self._processing_metadata and 'workflow_data' in self._processing_metadata:
                    workflow_data = self._processing_metadata['workflow_data']
                if workflow is not None and workflow_data is not None:
                    workflow.metadata = workflow_data
                    self._request_metadata['workflow'] = workflow
            if 'build_workflow' in self._request_metadata:
                build_workflow = self._request_metadata['build_workflow']
                build_workflow_data = None
                if self._processing_metadata and 'build_workflow_data' in self._processing_metadata:
                    build_workflow_data = self._processing_metadata['build_workflow_data']
                if build_workflow is not None and build_workflow_data is not None:
                    build_workflow.metadata = build_workflow_data
                    self._request_metadata['build_workflow'] = build_workflow
        return self._request_metadata

    @request_metadata.setter
    def request_metadata(self, request_metadata):
        if self._request_metadata is None:
            self._request_metadata = request_metadata
        if self._processing_metadata is None:
            self._processing_metadata = {}
        if request_metadata:
            if 'workflow' in request_metadata:
                workflow = request_metadata['workflow']
                self._processing_metadata['workflow_data'] = workflow.metadata
            if 'build_workflow' in request_metadata:
                build_workflow = request_metadata['build_workflow']
                self._processing_metadata['build_workflow_data'] = build_workflow.metadata

    @property
    def processing_metadata(self):
        return self._processing_metadata

    @processing_metadata.setter
    def processing_metadata(self, processing_metadata):
        if self._processing_metadata is None:
            self._processing_metadata = {}
        if processing_metadata:
            for k in processing_metadata:
                if k != 'workflow_data' and k != 'build_workflow_data':
                    self._processing_metadata[k] = processing_metadata[k]

    def _items_extend(self):
        return [('request_metadata', self.request_metadata),
                ('processing_metadata', self.processing_metadata)]

    def update(self, values, flush=True, session=None):
        if values and 'request_metadata' in values:
            if 'workflow' in values['request_metadata']:
                workflow = values['request_metadata']['workflow']

                if workflow is not None:
                    if 'processing_metadata' not in values:
                        values['processing_metadata'] = {}
                    values['processing_metadata']['workflow_data'] = workflow.metadata
            if 'build_workflow' in values['request_metadata']:
                build_workflow = values['request_metadata']['build_workflow']

                if build_workflow is not None:
                    if 'processing_metadata' not in values:
                        values['processing_metadata'] = {}
                    values['processing_metadata']['build_workflow_data'] = build_workflow.metadata

        if values and 'request_metadata' in values:
            del values['request_metadata']
        if values and 'processing_metadata' in values:
            values['_processing_metadata'] = values['processing_metadata']
            del values['processing_metadata']
        super(Request, self).update(values, flush, session)

    __table_args__ = (PrimaryKeyConstraint('request_id', name='REQUESTS_PK'),
                      CheckConstraint('status IS NOT NULL', name='REQUESTS_STATUS_ID_NN'),
                      # UniqueConstraint('name', 'scope', 'requester', 'request_type', 'transform_tag', 'workload_id', name='REQUESTS_NAME_SCOPE_UQ '),
                      Index('REQUESTS_SCOPE_NAME_IDX', 'name', 'scope', 'workload_id'),
                      Index('REQUESTS_STATUS_SITE', 'status', 'site', 'request_id'),
                      Index('REQUESTS_STATUS_PRIO_IDX', 'status', 'priority', 'request_id', 'locking', 'updated_at', 'next_poll_at', 'created_at'),
                      Index('REQUESTS_STATUS_POLL_IDX', 'status', 'priority', 'locking', 'updated_at', 'new_poll_period', 'update_poll_period', 'created_at', 'request_id'))


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
    errors = Column(JSONString(1024))
    workprogress_metadata = Column(JSON())
    processing_metadata = Column(JSON())

    __table_args__ = (PrimaryKeyConstraint('workprogress_id', name='WORKPROGRESS_PK'),
                      ForeignKeyConstraint(['request_id'], ['requests.request_id'], name='REQ2WORKPROGRESS_REQ_ID_FK'),
                      CheckConstraint('status IS NOT NULL', name='WORKPROGRESS_STATUS_ID_NN'),
                      # UniqueConstraint('name', 'scope', 'requester', 'request_type', 'transform_tag', 'workload_id', name='REQUESTS_NAME_SCOPE_UQ '),
                      Index('WORKPROGRESS_SCOPE_NAME_IDX', 'name', 'scope', 'workprogress_id'),
                      Index('WORKPROGRESS_STATUS_PRIO_IDX', 'status', 'priority', 'workprogress_id', 'locking', 'updated_at', 'next_poll_at', 'created_at'))


class Transform(BASE, ModelBase):
    """Represents a transform"""
    __tablename__ = 'transforms'
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('TRANSFORM_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    workload_id = Column(Integer())
    transform_type = Column(EnumWithValue(TransformType), nullable=False)
    transform_tag = Column(String(20))
    priority = Column(Integer())
    safe2get_output_from_input = Column(Integer())
    status = Column(EnumWithValue(TransformStatus), nullable=False)
    substatus = Column(EnumWithValue(TransformStatus), default=0)
    oldstatus = Column(EnumWithValue(TransformStatus), default=0)
    locking = Column(EnumWithValue(TransformLocking), nullable=False)
    retries = Column(Integer(), default=0)
    parent_transform_id = Column(BigInteger())
    previous_transform_id = Column(BigInteger())
    current_processing_id = Column(BigInteger())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    started_at = Column("started_at", DateTime)
    finished_at = Column("finished_at", DateTime)
    expired_at = Column("expired_at", DateTime)
    new_retries = Column(Integer(), default=0)
    update_retries = Column(Integer(), default=0)
    max_new_retries = Column(Integer(), default=3)
    max_update_retries = Column(Integer(), default=0)
    new_poll_period = Column(Interval(), default=datetime.timedelta(seconds=1))
    update_poll_period = Column(Interval(), default=datetime.timedelta(seconds=10))
    site = Column(String(50))
    name = Column(String(NAME_LENGTH))
    errors = Column(JSONString(1024))
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

    __table_args__ = (PrimaryKeyConstraint('transform_id', name='TRANSFORMS_PK'),
                      CheckConstraint('status IS NOT NULL', name='TRANSFORMS_STATUS_ID_NN'),
                      Index('TRANSFORMS_TYPE_TAG_IDX', 'transform_type', 'transform_tag', 'transform_id'),
                      Index('TRANSFORMS_STATUS_UPDATED_AT_IDX', 'status', 'locking', 'updated_at', 'next_poll_at', 'created_at'),
                      Index('TRANSFORMS_REQ_IDX', 'request_id', 'transform_id'),
                      Index('TRANSFORMS_STATUS_SITE', 'status', 'site', 'request_id', 'transform_id'),
                      Index('TRANSFORMS_STATUS_POLL_IDX', 'status', 'locking', 'updated_at', 'new_poll_period', 'update_poll_period', 'created_at', 'transform_id'))


class Workprogress2transform(BASE, ModelBase):
    """Represents a workprogress to transform"""
    __tablename__ = 'wp2transforms'
    workprogress_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)

    __table_args__ = (PrimaryKeyConstraint('workprogress_id', 'transform_id', name='WP2TRANSFORM_PK'),
                      ForeignKeyConstraint(['workprogress_id'], ['workprogresses.workprogress_id'], name='WP2TRANSFORM_WORK_ID_FK'),
                      ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='WP2TRANSFORM_TRANS_ID_FK'))


class Processing(BASE, ModelBase):
    """Represents a processing"""
    __tablename__ = 'processings'
    processing_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('PROCESSING_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    workload_id = Column(Integer())
    processing_type = Column(EnumWithValue(ProcessingType), nullable=False)
    status = Column(EnumWithValue(ProcessingStatus), nullable=False)
    substatus = Column(EnumWithValue(ProcessingStatus), default=0)
    oldstatus = Column(EnumWithValue(ProcessingStatus), default=0)
    locking = Column(EnumWithValue(ProcessingLocking), nullable=False)
    submitter = Column(String(20))
    submitted_id = Column(Integer())
    granularity = Column(Integer())
    granularity_type = Column(EnumWithValue(GranularityType))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    poller_updated_at = Column("poller_updated_at", DateTime, default=datetime.datetime.utcnow)
    submitted_at = Column("submitted_at", DateTime)
    finished_at = Column("finished_at", DateTime)
    expired_at = Column("expired_at", DateTime)
    new_retries = Column(Integer(), default=0)
    update_retries = Column(Integer(), default=0)
    max_new_retries = Column(Integer(), default=3)
    max_update_retries = Column(Integer(), default=0)
    new_poll_period = Column(Interval(), default=datetime.timedelta(seconds=1))
    update_poll_period = Column(Interval(), default=datetime.timedelta(seconds=10))
    site = Column(String(50))
    errors = Column(JSONString(1024))
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

    __table_args__ = (PrimaryKeyConstraint('processing_id', name='PROCESSINGS_PK'),
                      ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='PROCESSINGS_TRANSFORM_ID_FK'),
                      CheckConstraint('status IS NOT NULL', name='PROCESSINGS_STATUS_ID_NN'),
                      CheckConstraint('transform_id IS NOT NULL', name='PROCESSINGS_TRANSFORM_ID_NN'),
                      Index('PROCESSINGS_STATUS_SITE', 'status', 'site', 'request_id', 'transform_id', 'processing_id'),
                      Index('PROCESSINGS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'next_poll_at', 'created_at'),
                      Index('PROCESSINGS_STATUS_POLL_IDX', 'status', 'processing_id', 'locking', 'updated_at', 'new_poll_period', 'update_poll_period', 'created_at'))


class Collection(BASE, ModelBase):
    """Represents a collection"""
    __tablename__ = 'collections'
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('COLLECTION_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    workload_id = Column(Integer())
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    coll_type = Column(EnumWithValue(CollectionType), nullable=False)
    relation_type = Column(EnumWithValue(CollectionRelationType), nullable=False)
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    bytes = Column(Integer())
    status = Column(EnumWithValue(CollectionStatus), nullable=False)
    substatus = Column(EnumWithValue(CollectionStatus), default=0)
    locking = Column(EnumWithValue(CollectionLocking), nullable=False)
    total_files = Column(Integer())
    storage_id = Column(Integer())
    new_files = Column(Integer())
    processed_files = Column(Integer())
    processing_files = Column(Integer())
    failed_files = Column(Integer())
    missing_files = Column(Integer())
    ext_files = Column(Integer())
    processed_ext_files = Column(Integer())
    failed_ext_files = Column(Integer())
    missing_ext_files = Column(Integer())
    processing_id = Column(Integer())
    retries = Column(Integer(), default=0)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    next_poll_at = Column("next_poll_at", DateTime, default=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    coll_metadata = Column(JSON())

    __table_args__ = (PrimaryKeyConstraint('coll_id', name='COLLECTIONS_PK'),
                      UniqueConstraint('name', 'scope', 'transform_id', 'relation_type', name='COLLECTIONS_NAME_SCOPE_UQ'),
                      ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='COLLECTIONS_TRANSFORM_ID_FK'),
                      CheckConstraint('status IS NOT NULL', name='COLLECTIONS_STATUS_ID_NN'),
                      CheckConstraint('transform_id IS NOT NULL', name='COLLECTIONS_TRANSFORM_ID_NN'),
                      Index('COLLECTIONS_STATUS_RELAT_IDX', 'status', 'relation_type'),
                      Index('COLLECTIONS_TRANSFORM_IDX', 'transform_id', 'coll_id'),
                      Index('COLLECTIONS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'next_poll_at', 'created_at'),
                      Index('COLLECTIONS_REQ_IDX', 'request_id', 'transform_id', 'updated_at'),)


class Content(BASE, ModelBase):
    """Represents a content"""
    __tablename__ = 'contents'
    content_id = Column(BigInteger().with_variant(Integer, "sqlite"), Sequence('CONTENT_ID_SEQ', schema=DEFAULT_SCHEMA_NAME), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    workload_id = Column(Integer())
    map_id = Column(BigInteger().with_variant(Integer, "sqlite"), default=0, nullable=False)
    sub_map_id = Column(BigInteger().with_variant(Integer, "sqlite"), default=0)
    dep_sub_map_id = Column(BigInteger().with_variant(Integer, "sqlite"), default=0)
    content_dep_id = Column(BigInteger())
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(LONG_NAME_LENGTH))
    name_md5 = Column(String(33))
    scope_name_md5 = Column(String(33))
    min_id = Column(Integer(), default=0)
    max_id = Column(Integer(), default=0)
    content_type = Column(EnumWithValue(ContentType), nullable=False)
    content_relation_type = Column(EnumWithValue(ContentRelationType), default=0, nullable=False)
    status = Column(EnumWithValue(ContentStatus), nullable=False)
    substatus = Column(EnumWithValue(ContentStatus))
    locking = Column(EnumWithValue(ContentLocking), nullable=False)
    bytes = Column(Integer())
    md5 = Column(String(32))
    adler32 = Column(String(8))
    processing_id = Column(Integer())
    storage_id = Column(Integer())
    retries = Column(Integer(), default=0)
    external_coll_id = Column(BigInteger())
    external_content_id = Column(BigInteger())
    external_event_id = Column(BigInteger())
    external_event_status = Column(EnumWithValue(ContentStatus))
    path = Column(String(4000))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    accessed_at = Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    expired_at = Column("expired_at", DateTime)
    content_metadata = Column(JSONString(1000))

    __table_args__ = (PrimaryKeyConstraint('content_id', name='CONTENTS_PK'),
                      # UniqueConstraint('name', 'scope', 'coll_id', 'content_type', 'min_id', 'max_id', name='CONTENT_SCOPE_NAME_UQ'),
                      # UniqueConstraint('name', 'scope', 'coll_id', 'min_id', 'max_id', name='CONTENT_SCOPE_NAME_UQ'),
                      # UniqueConstraint('content_id', 'coll_id', name='CONTENTS_UQ'),
                      # UniqueConstraint('transform_id', 'coll_id', 'map_id', 'name', 'min_id', 'max_id', name='CONTENT_ID_UQ'),
                      UniqueConstraint('transform_id', 'coll_id', 'map_id', 'sub_map_id', 'dep_sub_map_id', 'content_relation_type', 'name_md5', 'scope_name_md5', 'min_id', 'max_id', name='CONTENT_ID_UQ'),
                      ForeignKeyConstraint(['transform_id'], ['transforms.transform_id'], name='CONTENTS_TRANSFORM_ID_FK'),
                      ForeignKeyConstraint(['coll_id'], ['collections.coll_id'], name='CONTENTS_COLL_ID_FK'),
                      CheckConstraint('status IS NOT NULL', name='CONTENTS_STATUS_ID_NN'),
                      CheckConstraint('coll_id IS NOT NULL', name='CONTENTS_COLL_ID_NN'),
                      Index('CONTENTS_STATUS_UPDATED_IDX', 'status', 'locking', 'updated_at', 'created_at'),
                      Index('CONTENTS_ID_NAME_IDX', 'coll_id', 'scope', func.md5('name'), 'status'),
                      Index('CONTENTS_DEP_IDX', 'request_id', 'transform_id', 'content_dep_id'),
                      Index('CONTENTS_REL_IDX', 'request_id', 'content_relation_type', 'transform_id', 'substatus'),
                      Index('CONTENTS_TF_IDX', 'transform_id', 'request_id', 'coll_id', 'map_id', 'content_relation_type'),
                      Index('CONTENTS_REQ_TF_COLL_IDX', 'request_id', 'transform_id', 'workload_id', 'coll_id', 'content_relation_type', 'status', 'substatus'))


class Content_update(BASE, ModelBase):
    """Represents a content update"""
    __tablename__ = 'contents_update'
    content_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    substatus = Column(EnumWithValue(ContentStatus))
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    fetch_status = Column(EnumWithValue(ContentFetchStatus), default=0, nullable=False)
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    content_metadata = Column(JSONString(100))


class Content_ext(BASE, ModelBase):
    """Represents a content extension"""
    __tablename__ = 'contents_ext'
    content_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    transform_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    coll_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    workload_id = Column(Integer())
    map_id = Column(BigInteger().with_variant(Integer, "sqlite"), default=0, nullable=False)
    status = Column(EnumWithValue(ContentStatus), nullable=False)
    panda_id = Column(BigInteger())
    job_definition_id = Column(BigInteger())
    scheduler_id = Column(String(128))
    pilot_id = Column(String(200))
    creation_time = Column(DateTime)
    modification_time = Column(DateTime)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    prod_source_label = Column(String(20))
    prod_user_id = Column(String(250))
    assigned_priority = Column(Integer())
    current_priority = Column(Integer())
    attempt_nr = Column(Integer())
    max_attempt = Column(Integer())
    max_cpu_count = Column(Integer())
    max_cpu_unit = Column(String(32))
    max_disk_count = Column(Integer())
    max_disk_unit = Column(String(10))
    min_ram_count = Column(Integer())
    min_ram_unit = Column(String(10))
    cpu_consumption_time = Column(Integer())
    cpu_consumption_unit = Column(String(128))
    job_status = Column(String(10))
    job_name = Column(String(255))
    trans_exit_code = Column(Integer())
    pilot_error_code = Column(Integer())
    pilot_error_diag = Column(String(500))
    exe_error_code = Column(Integer())
    exe_error_diag = Column(String(500))
    sup_error_code = Column(Integer())
    sup_error_diag = Column(String(250))
    ddm_error_code = Column(Integer())
    ddm_error_diag = Column(String(500))
    brokerage_error_code = Column(Integer())
    brokerage_error_diag = Column(String(250))
    job_dispatcher_error_code = Column(Integer())
    job_dispatcher_error_diag = Column(String(250))
    task_buffer_error_code = Column(Integer())
    task_buffer_error_diag = Column(String(300))
    computing_site = Column(String(128))
    computing_element = Column(String(128))
    grid = Column(String(50))
    cloud = Column(String(50))
    cpu_conversion = Column(Float())
    task_id = Column(BigInteger())
    vo = Column(String(16))
    pilot_timing = Column(String(100))
    working_group = Column(String(20))
    processing_type = Column(String(64))
    prod_user_name = Column(String(60))
    core_count = Column(Integer())
    n_input_files = Column(Integer())
    req_id = Column(BigInteger())
    jedi_task_id = Column(BigInteger())
    actual_core_count = Column(Integer())
    max_rss = Column(Integer())
    max_vmem = Column(Integer())
    max_swap = Column(Integer())
    max_pss = Column(Integer())
    avg_rss = Column(Integer())
    avg_vmem = Column(Integer())
    avg_swap = Column(Integer())
    avg_pss = Column(Integer())
    max_walltime = Column(Integer())
    disk_io = Column(Integer())
    failed_attempt = Column(Integer())
    hs06 = Column(Integer())
    hs06sec = Column(Integer())
    memory_leak = Column(String(10))
    memory_leak_x2 = Column(String(10))
    job_label = Column(String(20))

    __table_args__ = (PrimaryKeyConstraint('content_id', name='CONTENTS_EXT_PK'),
                      Index('CONTENTS_EXT_RTF_IDX', 'request_id', 'transform_id', 'workload_id', 'coll_id', 'content_id', 'panda_id', 'status'),
                      Index('CONTENTS_EXT_RTW_IDX', 'request_id', 'transform_id', 'workload_id'),
                      Index('CONTENTS_EXT_RTM_IDX', 'request_id', 'transform_id', 'map_id'))


class Health(BASE, ModelBase):
    """Represents the status of the running agents"""
    __tablename__ = 'health'
    health_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                       Sequence('HEALTH_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                       primary_key=True)
    agent = Column(String(30))
    hostname = Column(String(500))
    pid = Column(Integer, autoincrement=False)
    status = Column(EnumWithValue(HealthStatus), default=0, nullable=False)
    thread_id = Column(BigInteger, autoincrement=False)
    thread_name = Column(String(255))
    payload = Column(String(2048))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    payload = Column(String(2048))
    __table_args__ = (PrimaryKeyConstraint('health_id', name='HEALTH_PK'),
                      UniqueConstraint('agent', 'hostname', 'pid', 'thread_id', name='HEALTH_UK'))


class Message(BASE, ModelBase):
    """Represents the event messages"""
    __tablename__ = 'messages'
    msg_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                    Sequence('MESSAGE_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                    primary_key=True)
    msg_type = Column(EnumWithValue(MessageType), nullable=False)
    status = Column(EnumWithValue(MessageStatus), nullable=False)
    substatus = Column(Integer())
    locking = Column(EnumWithValue(MessageLocking), nullable=False)
    source = Column(EnumWithValue(MessageSource), nullable=False)
    destination = Column(EnumWithValue(MessageDestination), nullable=False)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"))
    workload_id = Column(Integer())
    transform_id = Column(Integer())
    processing_id = Column(Integer())
    num_contents = Column(Integer())
    retries = Column(Integer(), default=0)
    fetching_id = Column(Integer())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    poll_period = Column(Interval(), default=datetime.timedelta(seconds=300), nullable=False)
    msg_content = Column(JSON())

    __table_args__ = (PrimaryKeyConstraint('msg_id', name='MESSAGES_PK'),
                      Index('MESSAGES_TYPE_ST_IDX', 'msg_type', 'status', 'destination', 'request_id'),
                      Index('MESSAGES_TYPE_ST_TF_IDX', 'msg_type', 'status', 'destination', 'transform_id'),
                      Index('MESSAGES_TYPE_ST_PR_IDX', 'msg_type', 'status', 'destination', 'processing_id'),
                      Index('MESSAGES_ST_IDX', 'status', 'destination', 'created_at'),
                      Index('MESSAGES_TYPE_STU_IDX', 'msg_type', 'status', 'destination', 'retries', 'updated_at', 'created_at'))


class Command(BASE, ModelBase):
    """Represents the operations commands"""
    __tablename__ = 'commands'
    cmd_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                    Sequence('COMMAND_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                    primary_key=True)
    request_id = Column(BigInteger().with_variant(Integer, "sqlite"), nullable=False)
    workload_id = Column(Integer())
    transform_id = Column(Integer())
    processing_id = Column(Integer())
    cmd_type = Column(EnumWithValue(CommandType))
    status = Column(EnumWithValue(CommandStatus), nullable=False)
    substatus = Column(Integer())
    locking = Column(EnumWithValue(CommandLocking), nullable=False)
    username = Column(String(50))
    retries = Column(Integer(), default=0)
    source = Column(EnumWithValue(CommandLocation))
    destination = Column(EnumWithValue(CommandLocation))
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    cmd_content = Column(JSON())
    errors = Column(JSONString(1024))

    __table_args__ = (PrimaryKeyConstraint('cmd_id', name='COMMANDS_PK'),
                      Index('COMMANDS_TYPE_ST_IDX', 'cmd_type', 'status', 'destination', 'request_id'),
                      Index('COMMANDS_TYPE_ST_TF_IDX', 'cmd_type', 'status', 'destination', 'transform_id'),
                      Index('COMMANDS_TYPE_ST_PR_IDX', 'cmd_type', 'status', 'destination', 'processing_id'),
                      Index('COMMANDS_STATUS_IDX', 'status', 'locking', 'updated_at'))


class EventPriority(BASE, ModelBase):
    """Represents the operations events"""
    __tablename__ = 'events_priority'
    event_type = Column(EnumWithValue(EventType), primary_key=True, nullable=False)
    event_actual_id = Column(Integer(), primary_key=True, nullable=False)
    priority = Column(Integer(), default=1000, nullable=False)
    last_processed_at = Column("last_processed_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)

    __table_args__ = (PrimaryKeyConstraint('event_type', 'event_actual_id', name='EVENTS_PR_PK'),)


class Event(BASE, ModelBase):
    """Represents the operations events"""
    __tablename__ = 'events'
    event_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                      Sequence('EVENT_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                      primary_key=True)
    event_type = Column(EnumWithValue(EventType), nullable=False)
    event_actual_id = Column(Integer(), nullable=False)
    priority = Column(Integer())
    status = Column(EnumWithValue(EventStatus), nullable=False)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    processing_at = Column("processing_at", DateTime, default=None)
    processed_at = Column("processed_at", DateTime, default=None)
    content = Column(JSON())

    @property
    def _id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._id
        return None

    @property
    def _publisher_id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._publisher_id
        return None

    @property
    def _event_type(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._event_type
        return None

    @property
    def _timestamp(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._timestamp
        return None

    @property
    def _counter(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._counter
        return None

    @property
    def _content(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._content
        return None

    @property
    def has_changes(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event'].has_changes
        return None

    def get_event_id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event'].get_event_id()
        return None

    def able_to_merge(self, event):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event'].able_to_merge(event)
        return False

    def changed(self):
        return self.has_changes

    def merge(self, event):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event'].merge(event)
        return False, event

    @property
    def _request_id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._request_id
        return None

    @property
    def _command_id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._command_id
        return None

    @property
    def _transform_id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._transform_id
        return None

    @property
    def _processing_id(self):
        if self.content and 'event' in self.content and self.content['event']:
            return self.content['event']._processing_id
        return None

    __table_args__ = (PrimaryKeyConstraint('event_id', name='EVENTS_PK'),)


class EventArchive(BASE, ModelBase):
    """Represents the operations events"""
    __tablename__ = 'events_archive'
    event_id = Column(BigInteger(), primary_key=True)
    event_type = Column(EnumWithValue(EventType), nullable=False)
    event_actual_id = Column(Integer(), nullable=False)
    priority = Column(Integer())
    status = Column(EnumWithValue(EventStatus), nullable=False)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    processing_at = Column("processing_at", DateTime, default=None)
    processed_at = Column("processed_at", DateTime, default=None)
    content = Column(JSON())

    __table_args__ = (PrimaryKeyConstraint('event_id', name='EVENTS_AR_PK'),)


class Throttler(BASE, ModelBase):
    """Represents the throttlers"""
    __tablename__ = 'throttlers'
    throttler_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                          Sequence('THROTTLER_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                          primary_key=True)
    site = Column(String(50), nullable=False)
    status = Column(EnumWithValue(ThrottlerStatus), nullable=False)
    num_requests = Column(Integer())
    num_transforms = Column(Integer())
    num_processings = Column(Integer())
    new_contents = Column(Integer())
    queue_contents = Column(Integer())
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    others = Column(JSON())

    __table_args__ = (PrimaryKeyConstraint('throttler_id', name='THROTTLER_PK'),
                      UniqueConstraint('site', name='THROTTLER_SITE_UQ'))


class MetaInfo(BASE, ModelBase):
    """Represents the meta infos"""
    __tablename__ = 'meta_info'
    meta_id = Column(BigInteger().with_variant(Integer, "sqlite"),
                     Sequence('METAINFO_ID_SEQ', schema=DEFAULT_SCHEMA_NAME),
                     primary_key=True)
    name = Column(String(50), nullable=False)
    status = Column(EnumWithValue(MetaStatus), nullable=False)
    created_at = Column("created_at", DateTime, default=datetime.datetime.utcnow, nullable=False)
    updated_at = Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow, nullable=False)
    description = Column(String(1000), nullable=True)
    meta_info = Column(JSON())

    __table_args__ = (PrimaryKeyConstraint('meta_id', name='METAINFO_PK'),
                      UniqueConstraint('name', name='METAINFO_NAME_UQ'))


def create_trigger():
    func = DDL("""
        SET search_path TO %s;
        CREATE OR REPLACE FUNCTION update_dep_contents_status()
        RETURNS TRIGGER AS $$
        BEGIN
            UPDATE %s.contents set substatus = old.substatus where %s.contents.content_dep_id = old.content_id;
            RETURN OLD;
        END;
        $$ LANGUAGE PLPGSQL
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    trigger_ddl = DDL("""
        SET search_path TO %s;
        DROP TRIGGER IF EXISTS update_content_dep_status ON %s.contents_update;
        CREATE TRIGGER update_content_dep_status BEFORE DELETE ON %s.contents_update
        for each row EXECUTE PROCEDURE update_dep_contents_status();
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    event.listen(Content_update.__table__, "after_create", func.execute_if(dialect="postgresql"))
    event.listen(Content_update.__table__, "after_create", trigger_ddl.execute_if(dialect="postgresql"))


def delete_trigger():
    func = DDL("""
        SET search_path TO %s;
        DROP FUNCTION IF EXISTS update_dep_contents_status;
    """ % (DEFAULT_SCHEMA_NAME))
    trigger_ddl = DDL("""
        SET search_path TO %s;
        DROP TRIGGER IF EXISTS update_content_dep_status ON %s.contents_update;
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    event.listen(Content_update.__table__, "before_drop", func.execute_if(dialect="postgresql"))
    event.listen(Content_update.__table__, "before_drop", trigger_ddl.execute_if(dialect="postgresql"))


def create_func_to_update_contents():
    func1 = DDL("""
        SET search_path TO %s;
        CREATE OR REPLACE FUNCTION update_contents_to_others(request_id_in int, transform_id_in int)
        RETURNS INTEGER
        AS $$
        DECLARE num_rows INTEGER;
        BEGIN
            num_rows := 0;

            UPDATE %s.contents set substatus = d.substatus from
            (select content_id, content_dep_id, substatus from %s.contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 1 and status != 0) d
            where %s.contents.request_id = request_id_in and %s.contents.substatus != d.substatus and d.content_id = %s.contents.content_dep_id;
            GET DIAGNOSTICS num_rows = ROW_COUNT;
            return num_rows;
        END;
        $$ LANGUAGE PLPGSQL
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
           DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    func2 = DDL("""
        SET search_path TO %s;
        CREATE OR REPLACE FUNCTION update_contents_from_others(request_id_in int, transform_id_in int)
        RETURNS INTEGER
        AS $$
        DECLARE num_rows INTEGER;
        BEGIN
            num_rows := 0;

            UPDATE %s.contents set substatus = d.substatus from
            (select content_id, content_dep_id, substatus from %s.contents where request_id = request_id_in and content_relation_type = 1 and status != 0) d
            where %s.contents.request_id = request_id_in and %s.contents.transform_id = transform_id_in and %s.contents.substatus != d.substatus and d.content_id = %s.contents.content_dep_id;
            GET DIAGNOSTICS num_rows = ROW_COUNT;
            return num_rows;
        END;
        $$ LANGUAGE PLPGSQL
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
           DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    event.listen(Content.__table__, "after_create", func1.execute_if(dialect="postgresql"))
    event.listen(Content.__table__, "after_create", func2.execute_if(dialect="postgresql"))


def drop_func_to_update_contents():
    func = DDL("""
        SET search_path TO %s;
        DROP FUNCTION IF EXISTS update_contents_to_others;
        DROP FUNCTION IF EXISTS update_contents_from_others;
    """ % (DEFAULT_SCHEMA_NAME))
    event.listen(Content.__table__, "before_drop", func.execute_if(dialect="postgresql"))


def create_proc_to_update_contents():
    func1 = DDL("""
        SET search_path TO %s;
        CREATE OR REPLACE PROCEDURE update_contents_to_others(request_id_in int, transform_id_in int)
        AS $$
        BEGIN
            UPDATE %s.contents set substatus = d.substatus from
            (select content_id, content_dep_id, substatus from %s.contents where request_id = request_id_in and transform_id = transform_id_in and content_relation_type = 1 and status != 0) d
            where %s.contents.request_id = request_id_in and %s.contents.content_relation_type = 3 and %s.contents.substatus != d.substatus and d.content_id = %s.contents.content_dep_id;
        END;
        $$ LANGUAGE PLPGSQL
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
           DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    func2 = DDL("""
        SET search_path TO %s;
        CREATE OR REPLACE PROCEDURE update_contents_from_others(request_id_in int, transform_id_in int)
        AS $$
        BEGIN

            UPDATE %s.contents set substatus = d.substatus from
            (select content_id, content_dep_id, substatus from %s.contents where request_id = request_id_in and content_relation_type = 1 and status != 0) d
            where %s.contents.request_id = request_id_in and %s.contents.transform_id = transform_id_in and %s.contents.content_relation_type = 3 and %s.contents.substatus != d.substatus and d.content_id = %s.contents.content_dep_id;
        END;
        $$ LANGUAGE PLPGSQL
    """ % (DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
           DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SCHEMA_NAME))

    event.listen(Content.__table__, "after_create", func1.execute_if(dialect="postgresql"))
    event.listen(Content.__table__, "after_create", func2.execute_if(dialect="postgresql"))


def drop_proc_to_update_contents():
    func = DDL("""
        SET search_path TO %s;
        DROP PROCEDURE IF EXISTS update_contents_to_others;
        DROP PROCEDURE IF EXISTS update_contents_from_others;
    """ % (DEFAULT_SCHEMA_NAME))
    event.listen(Content.__table__, "before_drop", func.execute_if(dialect="postgresql"))


def get_request_sequence():
    seq = Sequence('REQUEST_ID_SEQ', schema=DEFAULT_SCHEMA_NAME, metadata=Request.metadata)
    # return seq.next_value().scalar()
    # return seq.next_value()
    return seq


def register_models(engine):
    """
    Creates database tables for all models with the given engine
    """

    # models = (Request, Workprogress, Transform, Workprogress2transform, Processing, Collection, Content, Health, Message)
    models = (Request, Transform, Processing, Collection, Content, Content_update, Content_ext, Health, Message, Command, Throttler)

    create_proc_to_update_contents()

    for model in models:
        # if not engine.has_table(model.__tablename__, model.metadata.schema):
        model.metadata.create_all(engine)   # pylint: disable=maybe-no-member


def unregister_models(engine):
    """
    Drops database tables for all models with the given engine
    """

    # models = (Request, Workprogress, Transform, Workprogress2transform, Processing, Collection, Content, Health, Message)
    models = (Request, Transform, Processing, Collection, Content, Content_update, Content_ext, Health, Message, Command, Throttler)

    drop_proc_to_update_contents()

    for model in models:
        model.metadata.drop_all(engine)   # pylint: disable=maybe-no-member
