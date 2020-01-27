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
Constants.
"""

from enum import Enum


SCOPE_LENGTH = 25
NAME_LENGTH = 255


class Sections:
    Main = 'main'
    Common = 'common'
    Clerk = 'clerk'
    Transformer = 'transformer'
    Transporter = 'transporter'
    Carrier = 'carrier'
    Conductor = 'conductor'


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


class RequestStatus(Enum):
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


class RequestType(Enum):
    Derivation = 0
    EventStreaming = 1
    StageIn = 2
    Other = 99


class TransformType(Enum):
    Derivation = 0
    EventStreaming = 1
    StageIn = 2
    Other = 99


class TransformStatus(Enum):
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


class CollectionType(Enum):
    Container = 0
    Dataset = 1
    File = 2


class CollectionRelationType(Enum):
    Input = 0
    Output = 1
    Log = 2


class CollectionStatus(Enum):
    New = 0
    Updated = 1
    Processing = 2
    Open = 3
    Closed = 4
    SubClosed = 5
    Failed = 6
    Deleted = 7


class ContentType(Enum):
    File = 0
    Event = 1


class ContentStatus(Enum):
    New = 0
    Processing = 1
    Available = 2
    Failed = 3
    FinalFailed = 4
    Lost = 5
    Deleted = 6


class GranularityType(Enum):
    File = 0
    Event = 1


class ProcessingStatus(Enum):
    New = 0
    Submitting = 1
    Submitted = 2
    Running = 3
    Finished = 4
    Failed = 5
    Lost = 6


class MessageType(Enum):
    StageInFile = 0
    StageInCollection = 1


class MessageStatus(Enum):
    New = 0
    Fetched = 1
    Delivered = 2


class MessageSource(Enum):
    Clerk = 0
    Transformer = 1
    Transporter = 2
    Carrier = 3
    Conductor = 4
