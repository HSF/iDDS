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
Constants.
"""

from enum import Enum


SCOPE_LENGTH = 25
NAME_LENGTH = 255
LONG_NAME_LENGTH = 4000


class Sections:
    Main = 'main'
    Common = 'common'
    Clerk = 'clerk'
    Marshaller = 'marshaller'
    Transformer = 'transformer'
    Transporter = 'transporter'
    Carrier = 'carrier'
    Conductor = 'conductor'
    Consumer = 'consumer'
    EventBus = 'eventbus'
    Cache = 'cache'
    Archiver = 'archiver'
    Coordinator = 'coordinator'


class HTTP_STATUS_CODE:
    OK = 200
    Created = 201
    Accepted = 202

    # Client Errors
    BadRequest = 400
    Unauthorized = 401
    Forbidden = 403
    NotFound = 404
    NoMethod = 405
    Conflict = 409

    # Server Errors
    InternalError = 500


class IDDSEnum(Enum):
    def to_dict(self):
        ret = {'class': self.__class__.__name__,
               'module': self.__class__.__module__,
               'attributes': {}}
        for key, value in self.__dict__.items():
            if not key.startswith('__'):
                if key == 'logger':
                    value = None
                if value and hasattr(value, 'to_dict'):
                    value = value.to_dict()
                ret['attributes'][key] = value
        return ret

    @staticmethod
    def is_class(d):
        if d and isinstance(d, dict) and 'class' in d and 'module' in d and 'attributes' in d:
            return True
        return False

    @staticmethod
    def load_instance(d):
        module = __import__(d['module'], fromlist=[None])
        cls = getattr(module, d['class'])
        if issubclass(cls, Enum):
            impl = cls(d['attributes']['_value_'])
        else:
            impl = cls()
        return impl

    @staticmethod
    def from_dict(d):
        if IDDSEnum.is_class(d):
            impl = IDDSEnum.load_instance(d)
            for key, value in d['attributes'].items():
                if key == 'logger':
                    continue
                if IDDSEnum.is_class(value):
                    value = IDDSEnum.from_dict(value)
                setattr(impl, key, value)
            return impl
        return d


class WorkflowType(IDDSEnum):
    Workflow = 0
    iWorkflow = 1
    iWork = 2


class WorkStatus(IDDSEnum):
    New = 0
    Ready = 1
    Transforming = 2
    Finished = 3
    SubFinished = 4
    Failed = 5
    Extend = 6
    ToCancel = 7
    Cancelling = 8
    Cancelled = 9
    ToSuspend = 10
    Suspending = 11
    Suspended = 12
    ToResume = 13
    Resuming = 14
    ToExpire = 15
    Expiring = 16
    Expired = 17
    ToFinish = 18
    ToForceFinish = 19
    Running = 20
    Terminating = 21


class RequestStatus(IDDSEnum):
    New = 0
    Ready = 1
    Transforming = 2
    Finished = 3
    SubFinished = 4
    Failed = 5
    Extend = 6
    ToCancel = 7
    Cancelling = 8
    Cancelled = 9
    ToSuspend = 10
    Suspending = 11
    Suspended = 12
    ToResume = 13
    Resuming = 14
    ToExpire = 15
    Expiring = 16
    Expired = 17
    ToFinish = 18
    ToForceFinish = 19
    Terminating = 20
    Building = 21
    Built = 22
    Throttling = 23


class RequestLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class WorkprogressStatus(IDDSEnum):
    New = 0
    Ready = 1
    Transforming = 2
    Finished = 3
    SubFinished = 4
    Failed = 5
    Extend = 6
    ToCancel = 7
    Cancelling = 8
    Cancelled = 9
    ToSuspend = 10
    Suspending = 11
    Suspended = 12
    ToResume = 13
    Resuming = 14
    ToExpire = 15
    Expiring = 16
    Expired = 17
    ToFinish = 18
    ToForceFinish = 19


class WorkprogressLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class RequestType(IDDSEnum):
    Workflow = 0
    EventStreaming = 1
    StageIn = 2
    ActiveLearning = 3
    HyperParameterOpt = 4
    Derivation = 5
    iWorkflow = 6
    Other = 99


class TransformType(IDDSEnum):
    Workflow = 0
    EventStreaming = 1
    StageIn = 2
    ActiveLearning = 3
    HyperParameterOpt = 4
    Derivation = 5
    Processing = 6
    Actuating = 7
    Data = 8
    iWorkflow = 9
    iWork = 10
    Other = 99


class TransformStatus(IDDSEnum):
    New = 0
    Ready = 1
    Transforming = 2
    Finished = 3
    SubFinished = 4
    Failed = 5
    Extend = 6
    ToCancel = 7
    Cancelling = 8
    Cancelled = 9
    ToSuspend = 10
    Suspending = 11
    Suspended = 12
    ToResume = 13
    Resuming = 14
    ToExpire = 15
    Expiring = 16
    Expired = 17
    ToFinish = 18
    ToForceFinish = 19
    Terminating = 20
    Building = 21
    Built = 22


class TransformLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class CollectionType(IDDSEnum):
    Container = 0
    Dataset = 1
    File = 2
    PseudoDataset = 3


class CollectionRelationType(IDDSEnum):
    Input = 0
    Output = 1
    Log = 2


class CollectionStatus(IDDSEnum):
    New = 0
    Updated = 1
    Processing = 2
    Open = 3
    Closed = 4
    SubClosed = 5
    Failed = 6
    Deleted = 7
    Cancelled = 8
    Suspended = 9


class CollectionLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class ContentType(IDDSEnum):
    File = 0
    Event = 1
    PseudoContent = 2


class ContentRelationType(IDDSEnum):
    Input = 0
    Output = 1
    Log = 2
    InputDependency = 3


class ContentStatus(IDDSEnum):
    New = 0
    Processing = 1
    Available = 2
    Failed = 3
    FinalFailed = 4
    Lost = 5
    Deleted = 6
    Mapped = 7
    FakeAvailable = 8
    Missing = 9
    Cancelled = 10
    Activated = 11
    SubAvailable = 12
    FinalSubAvailable = 13


class ContentLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class ContentFetchStatus(IDDSEnum):
    New = 0
    Fetching = 1
    Fetched = 2


class GranularityType(IDDSEnum):
    File = 0
    Event = 1


class ProcessingType(IDDSEnum):
    Workflow = 0
    EventStreaming = 1
    StageIn = 2
    ActiveLearning = 3
    HyperParameterOpt = 4
    Derivation = 5
    Processing = 6
    Actuating = 7
    Data = 8
    iWorkflow = 9
    iWork = 10
    Other = 99


class ProcessingStatus(IDDSEnum):
    New = 0
    Submitting = 1
    Submitted = 2
    Running = 3
    Finished = 4
    Failed = 5
    Lost = 6
    Cancel = 7
    FinishedOnStep = 8
    FinishedOnExec = 9
    FinishedTerm = 10
    SubFinished = 11
    ToCancel = 12
    Cancelling = 13
    Cancelled = 14
    ToSuspend = 15
    Suspending = 16
    Suspended = 17
    ToResume = 18
    Resuming = 19
    ToExpire = 20
    Expiring = 21
    Expired = 22
    TimeOut = 23
    ToFinish = 24
    ToForceFinish = 25
    Broken = 26
    Terminating = 27
    ToTrigger = 28
    Triggering = 29
    Synchronizing = 30


class ProcessingLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class HealthStatus(IDDSEnum):
    Default = 0
    InActive = 1
    Active = 2


class MessageType(IDDSEnum):
    StageInFile = 0
    StageInCollection = 1
    StageInWork = 2
    ActiveLearningFile = 3
    ActiveLearningCollection = 4
    ActiveLearningWork = 5
    HyperParameterOptFile = 6
    HyperParameterOptCollection = 7
    HyperParameterOptWork = 8
    ProcessingFile = 9
    ProcessingCollection = 10
    ProcessingWork = 11
    HealthHeartbeat = 12
    IDDSCommunication = 13
    ContentExt = 14
    UnknownFile = 97
    UnknownCollection = 98
    UnknownWork = 99


class MessageTypeStr(IDDSEnum):
    StageInFile = 'file_stagein'
    StageInCollection = 'collection_stagein'
    StageInWork = 'work_stagein'
    ActiveLearningFile = 'file_activelearning'
    ActiveLearningCollection = 'collection_activelearning'
    ActiveLearningWork = 'work_activelearning'
    HyperParameterOptFile = 'file_hyperparameteropt'
    HyperParameterOptCollection = 'collection_hyperparameteropt'
    HyperParameterOptWork = 'work_hyperparameteropt'
    ProcessingFile = 'file_processing'
    ProcessingCollection = 'collection_processing'
    ProcessingWork = 'work_processing'
    HealthHeartbeat = 'health_heartbeat'
    IDDSCommunication = 'idds_communication'
    UnknownFile = 'file_unknown'
    UnknownCollection = 'collection_unknown'
    UnknownWork = 'work_unknown'
    ContentExt = 'content_ext'


TransformType2MessageTypeMap = {
    '0': {'transform_type': TransformType.Workflow,
          'work': (MessageType.UnknownWork, MessageTypeStr.UnknownWork),
          'collection': (MessageType.UnknownCollection, MessageTypeStr.UnknownCollection),
          'file': (MessageType.UnknownFile, MessageTypeStr.UnknownFile)
          },
    '2': {'transform_type': TransformType.StageIn,
          'work': (MessageType.StageInWork, MessageTypeStr.StageInWork),
          'collection': (MessageType.StageInCollection, MessageTypeStr.StageInCollection),
          'file': (MessageType.StageInFile, MessageTypeStr.StageInFile)
          },
    '3': {'transform_type': TransformType.ActiveLearning,
          'work': (MessageType.ActiveLearningWork, MessageTypeStr.ActiveLearningWork),
          'collection': (MessageType.ActiveLearningCollection, MessageTypeStr.ActiveLearningCollection),
          'file': (MessageType.ActiveLearningFile, MessageTypeStr.ActiveLearningFile)
          },
    '4': {'transform_type': TransformType.HyperParameterOpt,
          'work': (MessageType.HyperParameterOptWork, MessageTypeStr.HyperParameterOptWork),
          'collection': (MessageType.HyperParameterOptCollection, MessageTypeStr.HyperParameterOptCollection),
          'file': (MessageType.HyperParameterOptFile, MessageTypeStr.HyperParameterOptFile)
          },
    '6': {'transform_type': TransformType.Processing,
          'work': (MessageType.ProcessingWork, MessageTypeStr.ProcessingWork),
          'collection': (MessageType.ProcessingCollection, MessageTypeStr.ProcessingCollection),
          'file': (MessageType.ProcessingFile, MessageTypeStr.ProcessingFile)
          }
}


class MessageStatus(IDDSEnum):
    New = 0
    Fetched = 1
    Delivered = 2
    Failed = 3
    ConfirmDelivered = 4


class MessageLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class MessageSource(IDDSEnum):
    Clerk = 0
    Transformer = 1
    Transporter = 2
    Carrier = 3
    Conductor = 4
    Rest = 5


class MessageDestination(IDDSEnum):
    Clerk = 0
    Transformer = 1
    Transporter = 2
    Carrier = 3
    Conductor = 4
    Outside = 5
    ContentExt = 6


class CommandType(IDDSEnum):
    AbortRequest = 0
    ResumeRequest = 1
    ExpireRequest = 2


class CommandStatus(IDDSEnum):
    New = 0
    Processing = 1
    Processed = 2
    Failed = 3
    UnknownCommand = 4


class MetaStatus(IDDSEnum):
    UnActive = 0
    Active = 1


class CommandLocking(IDDSEnum):
    Idle = 0
    Locking = 1


class CommandLocation(IDDSEnum):
    Clerk = 0
    Transformer = 1
    Transporter = 2
    Carrier = 3
    Conductor = 4
    Rest = 5
    Other = 6


class ThrottlerStatus(IDDSEnum):
    InActive = 0
    Active = 1


class ReturnCode(IDDSEnum):
    Ok = 0
    Failed = 255
    Locked = 1


def get_work_status_from_transform_processing_status(status):
    if status in [ProcessingStatus.New, TransformStatus.New]:
        return WorkStatus.New
    elif status in [ProcessingStatus.Submitting, ProcessingStatus.Submitted, TransformStatus.Transforming]:
        return WorkStatus.Transforming
    elif status in [ProcessingStatus.Running]:
        return WorkStatus.Transforming
    elif status in [ProcessingStatus.Finished, TransformStatus.Finished]:
        return WorkStatus.Finished
    elif status in [ProcessingStatus.Failed, ProcessingStatus.Broken, TransformStatus.Failed]:
        return WorkStatus.Failed
    elif status in [ProcessingStatus.SubFinished, TransformStatus.SubFinished]:
        return WorkStatus.SubFinished
    elif status in [ProcessingStatus.Cancelled, ProcessingStatus.Suspended, TransformStatus.Cancelled, TransformStatus.Suspended]:
        return WorkStatus.Cancelled
    elif status in [ProcessingStatus.Terminating, TransformStatus.Terminating]:
        return WorkStatus.Terminating
    else:
        return WorkStatus.Transforming


def get_transform_status_from_processing_status(status):
    map = {ProcessingStatus.New: TransformStatus.Transforming,       # when Processing is created, set it to transforming
           ProcessingStatus.Submitting: TransformStatus.Transforming,
           ProcessingStatus.Submitted: TransformStatus.Transforming,
           ProcessingStatus.Running: TransformStatus.Transforming,
           ProcessingStatus.Finished: TransformStatus.Finished,
           ProcessingStatus.Failed: TransformStatus.Failed,
           ProcessingStatus.Lost: TransformStatus.Failed,
           ProcessingStatus.Cancel: TransformStatus.Cancelled,
           ProcessingStatus.FinishedOnStep: TransformStatus.Finished,
           ProcessingStatus.FinishedOnExec: TransformStatus.Finished,
           ProcessingStatus.FinishedTerm: TransformStatus.Finished,
           ProcessingStatus.SubFinished: TransformStatus.SubFinished,
           ProcessingStatus.ToCancel: TransformStatus.ToCancel,
           ProcessingStatus.Cancelling: TransformStatus.Cancelling,
           ProcessingStatus.Cancelled: TransformStatus.Cancelled,
           ProcessingStatus.ToSuspend: TransformStatus.ToSuspend,
           ProcessingStatus.Suspending: TransformStatus.Suspending,
           ProcessingStatus.Suspended: TransformStatus.Suspended,
           ProcessingStatus.ToResume: TransformStatus.ToResume,
           ProcessingStatus.Resuming: TransformStatus.Resuming,
           ProcessingStatus.ToExpire: TransformStatus.ToExpire,
           ProcessingStatus.Expiring: TransformStatus.Expiring,
           ProcessingStatus.Expired: TransformStatus.Expired,
           ProcessingStatus.TimeOut: TransformStatus.Failed,
           ProcessingStatus.ToFinish: TransformStatus.ToFinish,
           ProcessingStatus.ToForceFinish: TransformStatus.ToForceFinish,
           ProcessingStatus.Broken: TransformStatus.Failed,
           ProcessingStatus.Terminating: TransformStatus.Terminating,
           ProcessingStatus.ToTrigger: TransformStatus.Transforming,
           ProcessingStatus.Triggering: TransformStatus.Transforming,
           ProcessingStatus.Synchronizing: TransformStatus.Transforming
           }
    if status in map:
        return map[status]
    return WorkStatus.Transforming


def get_processing_type_from_transform_type(tf_type):
    map = {TransformType.Workflow: ProcessingType.Workflow,
           TransformType.EventStreaming: ProcessingType.EventStreaming,
           TransformType.StageIn: ProcessingType.StageIn,
           TransformType.ActiveLearning: ProcessingType.ActiveLearning,
           TransformType.HyperParameterOpt: ProcessingType.HyperParameterOpt,
           TransformType.Derivation: ProcessingType.Derivation,
           TransformType.Processing: ProcessingType.Processing,
           TransformType.Actuating: ProcessingType.Actuating,
           TransformType.Data: ProcessingType.Data,
           TransformType.iWorkflow: ProcessingType.iWorkflow,
           TransformType.iWork: ProcessingType.iWork,
           TransformType.Other: ProcessingType.Other}
    if tf_type in map:
        return map[tf_type]
    return ProcessingType.Other
