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

    NewTransform = 20
    UpdateTransform = 21
    AbortTransform = 22
    ResumeTransfrom = 23

    NewProcessing = 30
    UpdateProcessing = 31
    AbortProcessing = 32
    ResumeProcessing = 33


class Event(object):
    def __init__(self, publisher_id, event_type=EventType.Event):
        self._id = str(uuid.uuid4())
        self._publisher_id = publisher_id
        self._event_type = event_type
        self._timestamp = time.time()


class StateClaimEvent(Event):
    def __init__(self, publisher_id, event_bus_state):
        super(StateClaimEvent).__init__(publisher_id, event_type=EventType.StateClaim)
        self._event_bus_state = event_bus_state


class DemandEvent(Event):
    def __init__(self, publisher_id, demand_type):
        super(StateClaimEvent).__init__(publisher_id, event_type=EventType.Demand)
        self._demand_type = demand_type


class NewRequestEvent(Event):
    def __init__(self, publisher_id, request_id):
        super(NewRequestEvent).__init__(publisher_id, event_type=EventType.NewRequest)
        self._request_id = request_id


class UpdateRequestEvent(Event):
    def __init__(self, publisher_id, request_id):
        super(UpdateRequestEvent).__init__(publisher_id, event_type=EventType.UpdateRequest)
        self._request_id = request_id


class AbortRequestEvent(Event):
    def __init__(self, publisher_id, request_id, stage='new'):
        super(AbortRequestEvent).__init__(publisher_id, event_type=EventType.AbortRequest)
        self._request_id = request_id
        self._stage = stage


class ResumeRequestEvent(Event):
    def __init__(self, publisher_id, request_id, stage='new'):
        super(ResumeRequestEvent).__init__(publisher_id, event_type=EventType.ResumeRequest)
        self._request_id = request_id
        self._stage = stage


class NewTransformEvent(Event):
    def __init__(self, publisher_id, transform_id):
        super(NewTransformEvent).__init__(publisher_id, event_type=EventType.NewTransform)
        self._transform_id = transform_id


class UpdateTransformEvent(Event):
    def __init__(self, publisher_id, transform_id):
        super(UpdateTransformEvent).__init__(publisher_id, event_type=EventType.UpdateRequest)
        self._transform_id = transform_id


class NewProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id):
        super(NewProcessingEvent).__init__(publisher_id, event_type=EventType.NewProcessing)
        self._processing_id = processing_id


class UpdateProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id):
        super(UpdateProcessingEvent).__init__(publisher_id, event_type=EventType.UpdateProcessing)
        self._processing_id = processing_id
