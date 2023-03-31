#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022 - 2023

import time
import uuid

from deepdiff import DeepDiff

from idds.common.constants import IDDSEnum
from idds.common.dict_class import DictClass
from idds.common.utils import json_dumps, merge_dict


class EventBusState(IDDSEnum):
    New = 0
    Master = 1
    Slave = 2
    Unknown = 3


class EventType(IDDSEnum):
    Event = 0
    StateClaim = 1
    Demand = 2
    Message = 3

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
    MsgTriggerProcessing = 37

    UpdateCommand = 40

    Test = 90


class EventPriority(IDDSEnum):
    Low = 0
    Medium = 10
    High = 50


class EventStatus(IDDSEnum):
    New = 0
    Processing = 1
    Processed = 2


class Event(DictClass):
    def __init__(self, publisher_id=None, event_type=EventType.Event, content=None, counter=1):
        self._id = str(uuid.uuid4())
        self._publisher_id = publisher_id
        self._event_type = event_type
        self._timestamp = time.time()
        self._counter = counter
        self._content = content
        self.has_changes = False
        self._requeue_counter = 0

    def get_event_id(self):
        return uuid.UUID(self._id).int % 1000000

    @property
    def event_type(self):
        return self._event_type.name

    def able_to_merge(self, event):
        if self._event_type == event._event_type and self.get_event_id() == event.get_event_id():
            return True
        if self._event_type == event._event_type and self.get_event_id() == event.get_event_id() and self._counter == event._counter:
            return True
            # if (self._content is None and event._content is None):
            #     return True
            # elif (self._content is not None and event._content is not None):
            #     ddiff = DeepDiff(self._content, event._content, ignore_order=True)
            #     if not ddiff:
            #         return True
        return False

    def changed(self):
        return self.has_changes

    def merge(self, event):
        self.has_changes = False
        if self.able_to_merge(event):
            if event._counter:
                if self._counter is None:
                    self._counter = event._counter
                    self.has_changes = True
                elif self._counter and event._counter and self._counter < event._counter:
                    self._counter = event._counter
                    self.has_changes = True

            if event._content:
                if self._content is None:
                    self._content = event._content
                    self.has_changes = True
                else:
                    ddiff = DeepDiff(self._content, event._content, ignore_order=True)
                    if ddiff:
                        self._content = merge_dict(self._content, event._content)
                        self.has_changes = True
            return True, None
        else:
            return False, event

    def requeue(self):
        self._requeue_counter += 1

    def get_requeue_counter(self):
        return self._requeue_counter

    def to_json(self, strip=False):
        ret = {'id': self._id, 'publisher_id': self._publisher_id,
               'event_type': (self._event_type.name, self._event_type.value),
               'timestamp': self._timestamp,
               'counter': self._counter,
               'content': self._content}
        return ret

    def __str__(self):
        return json_dumps(self.to_json())

    def clean(self):
        pass

    def fail(self):
        pass

    def set_terminating(self):
        if self._content is None:
            self._content = {}
        self._content['is_terminating'] = True

    def is_terminating(self):
        if self._content and ('is_terminating' in self._content and self._content['is_terminating']):
            return True

    def set_has_updates(self):
        if self._content is None:
            self._content = {}
        self._content['has_updates'] = True

    def has_updates(self):
        if self._content and ('has_updates' in self._content and self._content['has_updates']
            or 'num_to_update_contents' in self._content and self._content['num_to_update_contents']):     # noqa W503, E125, E128
            return True


class StateClaimEvent(Event):
    def __init__(self, publisher_id=None, event_bus_state=None, content=None, counter=1):
        super(StateClaimEvent, self).__init__(publisher_id, event_type=EventType.StateClaim, content=content, counter=counter)
        self._event_bus_state = event_bus_state

    def to_json(self, strip=False):
        ret = super(StateClaimEvent, self).to_json()
        ret['event_bus_state'] = self._event_bus_state
        return ret


class TestEvent(Event):
    def __init__(self, publisher_id=None, content=None, counter=1):
        super(TestEvent, self).__init__(publisher_id, event_type=EventType.Test, content=content, counter=counter)

    def to_json(self, strip=False):
        ret = super(TestEvent, self).to_json()
        return ret


class DemandEvent(Event):
    def __init__(self, publisher_id=None, demand_type=None, content=None, counter=1):
        super(DemandEvent, self).__init__(publisher_id, event_type=EventType.Demand, content=content, counter=counter)
        self._demand_type = demand_type

    def to_json(self, strip=False):
        ret = super(DemandEvent, self).to_json()
        ret['demand_type'] = self._demand_type
        return ret


class NewRequestEvent(Event):
    def __init__(self, publisher_id=None, request_id=None, content=None, counter=1):
        super(NewRequestEvent, self).__init__(publisher_id, event_type=EventType.NewRequest, content=content, counter=counter)
        self._request_id = request_id

    def get_event_id(self):
        return self._request_id

    def to_json(self, strip=False):
        ret = super(NewRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class UpdateRequestEvent(Event):
    def __init__(self, publisher_id=None, request_id=None, content=None, counter=1):
        super(UpdateRequestEvent, self).__init__(publisher_id, event_type=EventType.UpdateRequest, content=content, counter=counter)
        self._request_id = request_id

    def get_event_id(self):
        return self._request_id

    def to_json(self, strip=False):
        ret = super(UpdateRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class AbortRequestEvent(Event):
    def __init__(self, publisher_id=None, request_id=None, content=None, counter=1):
        super(AbortRequestEvent, self).__init__(publisher_id, event_type=EventType.AbortRequest, content=content, counter=counter)
        self._request_id = request_id

    def get_event_id(self):
        return self._request_id

    def to_json(self, strip=False):
        ret = super(AbortRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class ResumeRequestEvent(Event):
    def __init__(self, publisher_id=None, request_id=None, content=None, counter=1):
        super(ResumeRequestEvent, self).__init__(publisher_id, event_type=EventType.ResumeRequest, content=content, counter=counter)
        self._request_id = request_id

    def get_event_id(self):
        return self._request_id

    def to_json(self, strip=False):
        ret = super(ResumeRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class ExpireRequestEvent(Event):
    def __init__(self, publisher_id=None, request_id=None, content=None, counter=1):
        super(ExpireRequestEvent, self).__init__(publisher_id, event_type=EventType.ExpireRequest, content=content, counter=counter)
        self._request_id = request_id

    def get_event_id(self):
        return self._request_id

    def to_json(self, strip=False):
        ret = super(ExpireRequestEvent, self).to_json()
        ret['request_id'] = self._request_id
        return ret


class UpdateCommandEvent(Event):
    def __init__(self, publisher_id=None, command_id=None, content=None, counter=1):
        super(UpdateCommandEvent, self).__init__(publisher_id, event_type=EventType.UpdateCommand, content=content, counter=counter)
        self._command_id = command_id

    def get_event_id(self):
        return self._command_id

    def to_json(self, strip=False):
        ret = super(UpdateCommandEvent, self).to_json()
        ret['command_id'] = self._command_id
        return ret


class NewTransformEvent(Event):
    def __init__(self, publisher_id=None, transform_id=None, content=None, counter=1):
        super(NewTransformEvent, self).__init__(publisher_id, event_type=EventType.NewTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def get_event_id(self):
        return self._transform_id

    def to_json(self, strip=False):
        ret = super(NewTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class UpdateTransformEvent(Event):
    def __init__(self, publisher_id=None, transform_id=None, content=None, counter=1):
        super(UpdateTransformEvent, self).__init__(publisher_id, event_type=EventType.UpdateTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def get_event_id(self):
        return self._transform_id

    def to_json(self, strip=False):
        ret = super(UpdateTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class AbortTransformEvent(Event):
    def __init__(self, publisher_id=None, transform_id=None, content=None, counter=1):
        super(AbortTransformEvent, self).__init__(publisher_id, event_type=EventType.AbortTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def get_event_id(self):
        return self._transform_id

    def to_json(self, strip=False):
        ret = super(AbortTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class ResumeTransformEvent(Event):
    def __init__(self, publisher_id=None, transform_id=None, content=None, counter=1):
        super(ResumeTransformEvent, self).__init__(publisher_id, event_type=EventType.ResumeTransform, content=content, counter=counter)
        self._transform_id = transform_id

    def get_event_id(self):
        return self._transform_id

    def to_json(self, strip=False):
        ret = super(ResumeTransformEvent, self).to_json()
        ret['transform_id'] = self._transform_id
        return ret


class NewProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(NewProcessingEvent, self).__init__(publisher_id, event_type=EventType.NewProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(NewProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class UpdateProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(UpdateProcessingEvent, self).__init__(publisher_id, event_type=EventType.UpdateProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(UpdateProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class AbortProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(AbortProcessingEvent, self).__init__(publisher_id, event_type=EventType.AbortProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(AbortProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class ResumeProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(ResumeProcessingEvent, self).__init__(publisher_id, event_type=EventType.ResumeProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(ResumeProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class SyncProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(SyncProcessingEvent, self).__init__(publisher_id, event_type=EventType.SyncProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(SyncProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class TerminatedProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(TerminatedProcessingEvent, self).__init__(publisher_id, event_type=EventType.TerminatedProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(TerminatedProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class TriggerProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(TriggerProcessingEvent, self).__init__(publisher_id, event_type=EventType.TriggerProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(TriggerProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class MsgTriggerProcessingEvent(Event):
    def __init__(self, publisher_id=None, processing_id=None, content=None, counter=1):
        super(MsgTriggerProcessingEvent, self).__init__(publisher_id, event_type=EventType.MsgTriggerProcessing, content=content, counter=counter)
        self._processing_id = processing_id

    def get_event_id(self):
        return self._processing_id

    def to_json(self, strip=False):
        ret = super(MsgTriggerProcessingEvent, self).to_json()
        ret['processing_id'] = self._processing_id
        return ret


class MessageEvent(Event):
    def __init__(self, publisher_id=None, message=None, content=None, counter=1):
        super(MessageEvent, self).__init__(publisher_id, event_type=EventType.Message, content=content, counter=counter)
        self._msg = message

    def get_event_id(self):
        return uuid.UUID(self._id).int % 1000000

    def get_message(self):
        return self._msg

    def to_json(self, strip=False):
        ret = super(MessageEvent, self).to_json()
        if not strip:
            ret['message'] = self._msg
        return ret
