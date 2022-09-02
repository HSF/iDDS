#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

import json
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
    ResumeTransform = 23

    NewProcessing = 30
    UpdateProcessing = 31
    AbortProcessing = 32
    ResumeProcessing = 33
    SyncProcessing = 34
    TerminatedProcessing = 35


class Event(object):
    def __init__(self, publisher_id, event_type=EventType.Event, content=None):
        self._id = str(uuid.uuid4())
        self._publisher_id = publisher_id
        self._event_type = event_type
        self._timestamp = time.time()
        self._content = content

    def to_json(self):
        ret = {'id': self._id, 'publisher_id': self._publisher_id,
               'event_type': self._event_type, 'timestamp': self._timestamp,
               'content': self._content}
        return ret

    def __str__(self):
        return json.dumps(self.to_json())


class StateClaimEvent(Event):
    def __init__(self, publisher_id, event_bus_state, content=None):
        super(StateClaimEvent, self).__init__(publisher_id, event_type=EventType.StateClaim, content=content)
        self._event_bus_state = event_bus_state

    def to_json(self):
        ret = super(StateClaimEvent, self).to_json()
        ret['event_bus_state'] = self._event_bus_state
        return ret


class DemandEvent(Event):
    def __init__(self, publisher_id, demand_type, content=None):
        super(DemandEvent, self).__init__(publisher_id, event_type=EventType.Demand, content=content)
        self._demand_type = demand_type

    def to_json(self):
        ret = super(DemandEvent, self).to_json()
        ret['demand_type'] = self._demand_type
        return ret


class NewRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(NewRequestEvent, self).__init__(publisher_id, event_type=EventType.NewRequest, content=content)
        self._request_id = request_id

    def to_json(self):
        ret = super(NewRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class UpdateRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(UpdateRequestEvent, self).__init__(publisher_id, event_type=EventType.UpdateRequest, content=content)
        self._request_id = request_id

    def to_json(self):
        ret = super(UpdateRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class AbortRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(AbortRequestEvent, self).__init__(publisher_id, event_type=EventType.AbortRequest, content=content)
        self._request_id = request_id

    def to_json(self):
        ret = super(AbortRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class ResumeRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(ResumeRequestEvent, self).__init__(publisher_id, event_type=EventType.ResumeRequest, content=content)
        self._request_id = request_id

    def to_json(self):
        ret = super(ResumeRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class ExpireRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None):
        super(ExpireRequestEvent, self).__init__(publisher_id, event_type=EventType.ExpireRequest, content=content)
        self._request_id = request_id

    def to_json(self):
        ret = super(ExpireRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class NewTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(NewTransformEvent, self).__init__(publisher_id, event_type=EventType.NewTransform, content=content)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(NewTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class UpdateTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(UpdateTransformEvent, self).__init__(publisher_id, event_type=EventType.UpdateTransform, content=content)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(UpdateTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class AbortTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(AbortTransformEvent, self).__init__(publisher_id, event_type=EventType.AbortTransform, content=content)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(AbortTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class ResumeTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None):
        super(ResumeTransformEvent, self).__init__(publisher_id, event_type=EventType.ResumeTransform, content=content)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(ResumeTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class NewProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(NewProcessingEvent, self).__init__(publisher_id, event_type=EventType.NewProcessing, content=content)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(NewProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class UpdateProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(UpdateProcessingEvent, self).__init__(publisher_id, event_type=EventType.UpdateProcessing, content=content)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(UpdateProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class AbortProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(AbortProcessingEvent, self).__init__(publisher_id, event_type=EventType.AbortProcessing, content=content)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(AbortProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class ResumeProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(ResumeProcessingEvent, self).__init__(publisher_id, event_type=EventType.ResumeProcessing, content=content)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(ResumeProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class SyncProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(SyncProcessingEvent, self).__init__(publisher_id, event_type=EventType.SyncProcessing, content=content)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(SyncProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class TerminatedProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None):
        super(TerminatedProcessingEvent, self).__init__(publisher_id, event_type=EventType.TerminatedProcessing, content=content)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(TerminatedProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret
