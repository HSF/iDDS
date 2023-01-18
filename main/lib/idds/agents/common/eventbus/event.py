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

from idds.common.utils import json_dumps


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
    TriggerProcessing = 36

    UpdateCommand = 40


class Event(object):
    def __init__(self, publisher_id, event_type=EventType.Event, content=None, counter=1):
        self._id = str(uuid.uuid4())
        self._publisher_id = publisher_id
        self._event_type = event_type
        self._timestamp = time.time()
        self._counter = counter
        self._content = content

    def to_json(self):
        ret = {'id': self._id, 'publisher_id': self._publisher_id,
               'event_type': (self._event_type.name, self._event_type.value),
               'timestamp': self._timestamp,
               'counter': self._counter,
               'content': self._content}
        return ret

    def __str__(self):
        return json_dumps(self.to_json())


class StateClaimEvent(Event):
    def __init__(self, publisher_id, event_bus_state, content=None, counter=1):
        super(StateClaimEvent, self).__init__(publisher_id, event_type=EventType.StateClaim, content=content, counter=counter)
        self._event_bus_state = event_bus_state

    def to_json(self):
        ret = super(StateClaimEvent, self).to_json()
        ret['event_bus_state'] = self._event_bus_state
        return ret


class DemandEvent(Event):
    def __init__(self, publisher_id, demand_type, content=None, counter=1):
        super(DemandEvent, self).__init__(publisher_id, event_type=EventType.Demand, content=content, counter=counter)
        self._demand_type = demand_type

    def to_json(self):
        ret = super(DemandEvent, self).to_json()
        ret['demand_type'] = self._demand_type
        return ret


class NewRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None, counter=1):
        super(NewRequestEvent, self).__init__(publisher_id, event_type=EventType.NewRequest, content=content, counter=counter)
        self._request_id = request_id

    def to_json(self):
        ret = super(NewRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class UpdateRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None, counter=1):
        super(UpdateRequestEvent, self).__init__(publisher_id, event_type=EventType.UpdateRequest, content=content, counter=counter)
        self._request_id = request_id

    def to_json(self):
        ret = super(UpdateRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class AbortRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None, counter=1):
        super(AbortRequestEvent, self).__init__(publisher_id, event_type=EventType.AbortRequest, content=content, counter=counter)
        self._request_id = request_id

    def to_json(self):
        ret = super(AbortRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class ResumeRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None, counter=1):
        super(ResumeRequestEvent, self).__init__(publisher_id, event_type=EventType.ResumeRequest, content=content, counter=counter)
        self._request_id = request_id

    def to_json(self):
        ret = super(ResumeRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class ExpireRequestEvent(Event):
    def __init__(self, publisher_id, request_id, content=None, counter=1):
        super(ExpireRequestEvent, self).__init__(publisher_id, event_type=EventType.ExpireRequest, content=content, counter=counter)
        self._request_id = request_id

    def to_json(self):
        ret = super(ExpireRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class UpdateCommandEvent(Event):
    def __init__(self, publisher_id, command_id, content=None, counter=1):
        super(UpdateCommandEvent, self).__init__(publisher_id, event_type=EventType.UpdateCommand, content=content, counter=counter)
        self._command_id = command_id

    def to_json(self):
        ret = super(UpdateCommandEvent, self).to_json()
        ret['command_id'] = self._command_id
        return ret


class NewTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None, counter=1):
        super(NewTransformEvent, self).__init__(publisher_id, event_type=EventType.NewTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(NewTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class UpdateTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None, counter=1):
        super(UpdateTransformEvent, self).__init__(publisher_id, event_type=EventType.UpdateTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(UpdateTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class AbortTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None, counter=1):
        super(AbortTransformEvent, self).__init__(publisher_id, event_type=EventType.AbortTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(AbortTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class ResumeTransformEvent(Event):
    def __init__(self, publisher_id, transform_id, content=None, counter=1):
        super(ResumeTransformEvent, self).__init__(publisher_id, event_type=EventType.ResumeTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def to_json(self):
        ret = super(ResumeTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class NewProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(NewProcessingEvent, self).__init__(publisher_id, event_type=EventType.NewProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(NewProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class UpdateProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(UpdateProcessingEvent, self).__init__(publisher_id, event_type=EventType.UpdateProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(UpdateProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class AbortProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(AbortProcessingEvent, self).__init__(publisher_id, event_type=EventType.AbortProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(AbortProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class ResumeProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(ResumeProcessingEvent, self).__init__(publisher_id, event_type=EventType.ResumeProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(ResumeProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class SyncProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(SyncProcessingEvent, self).__init__(publisher_id, event_type=EventType.SyncProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(SyncProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class TerminatedProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(TerminatedProcessingEvent, self).__init__(publisher_id, event_type=EventType.TerminatedProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(TerminatedProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class TriggerProcessingEvent(Event):
    def __init__(self, publisher_id, processing_id, content=None, counter=1):
        super(TriggerProcessingEvent, self).__init__(publisher_id, event_type=EventType.TriggerProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def to_json(self):
        ret = super(TriggerProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret
