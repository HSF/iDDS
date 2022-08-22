#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

import time
import uuid

from enum import Enum


class EventBusState(Enum):
    New = 0
    Master = 1
    Slave = 2
    Unknown = 3


class EventType(Enum):
    Event = 0
    StateClaim = 1
    Demand = 2

    NewRequest = 10
    UpdateRequest = 11
    AbortRequest = 12
    ResumeRequest = 13
    ExpireRequest = 14

    NewTransform = 20
    UpdateTransform = 21
    AbortTransform = 22
    ResumeTransfrom = 23

    NewProcessing = 30
    UpdateProcessing = 31
    AbortProcessing = 32
    ResumeProcessing = 33


class Event(object):
    def __init__(self, publisher_id, event_type=EventType.Event, content=None):
        self._id = str(uuid.uuid4())
        self._publisher_id = publisher_id
        self._event_type = event_type
        self._timestamp = time.time()
        self._content = content


class StateClaimEvent(Event):
    def __init__(self, publisher_id, event_bus_state, content=None):
        super(StateClaimEvent).__init__(publisher_id, event_type=EventType.StateClaim, content=content)
        self._event_bus_state = event_bus_state


class DemandEvent(Event):
    def __init__(self, publisher_id, demand_type, content=None):
        super(StateClaimEvent).__init__(publisher_id, event_type=EventType.Demand, content=content)
        self._demand_type = demand_type


class NewRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(NewRequestEvent).__init__(publisher_id, event_type=EventType.NewRequest, content=content)
        self._request_id = request_id


class UpdateRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(UpdateRequestEvent).__init__(publisher_id, event_type=EventType.UpdateRequest, content=content)
        self._request_id = request_id


class AbortRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(AbortRequestEvent).__init__(publisher_id, event_type=EventType.AbortRequest, content=content)
        self._request_id = request_id


class ResumeRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(ResumeRequestEvent).__init__(publisher_id, event_type=EventType.ResumeRequest, content=content)
        self._request_id = request_id


class ExpireRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(ExpireRequestEvent).__init__(publisher_id, event_type=EventType.ExpireRequest, content=content)
        self._request_id = request_id


class NewTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(NewTransformEvent).__init__(publisher_id, event_type=EventType.NewTransform, content=content)
        self._transform_id = transform_id


class UpdateTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(UpdateTransformEvent).__init__(publisher_id, event_type=EventType.UpdateTransform, content=content)
        self._transform_id = transform_id


class AbortTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(AbortTransformEvent).__init__(publisher_id, event_type=EventType.AbortTransform, content=content)
        self._transform_id = transform_id


class ResumeTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(ResumeTransformEvent).__init__(publisher_id, event_type=EventType.ResumeTransform, content=content)
        self._transform_id = transform_id


class NewProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(NewProcessingEvent).__init__(publisher_id, event_type=EventType.NewProcessing, content=content)
        self._processing_id = processing_id


class UpdateProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(UpdateProcessingEvent).__init__(publisher_id, event_type=EventType.UpdateProcessing, content=content)
        self._processing_id = processing_id


class AbortProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(AbortProcessingEvent).__init__(publisher_id, event_type=EventType.AbortProcessing, content=content)
        self._processing_id = processing_id


class ResumeProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(ResumeProcessingEvent).__init__(publisher_id, event_type=EventType.ResumeProcessing, content=content)
        self._processing_id = processing_id
