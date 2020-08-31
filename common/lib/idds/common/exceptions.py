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
IDDS Exceptions.

error codes:
    The fist the number is one of the main catagories.
    The second number is one of the subcatagories.
    The third number and numbers after third one are local defined for every subcatagory.

Catagories:
 1. common/unknown IDDS exception
 2. ORM related exception
    1. request table related exception
    2. transform table related exception
    3. collection table related exception
    4. collection_content table related exception
    5. processing table related exception
    6. messages table related exception
 3. core related exception
 4. Rest related exception
    1. bad request
    2. connection error
 5. Agent exception
    1. No plugin exception
    2. Agent plugin exception
"""


import traceback


class IDDSException(Exception):
    """
    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """

    def __init__(self, *args, **kwargs):
        super(IDDSException, self).__init__()
        self._message = "An unknown IDDS exception occurred."
        self.args = args
        self.kwargs = kwargs
        self.error_code = 100
        self._error_string = None

    def construct_error_string(self):
        try:
            self._error_string = "%s: %s" % (self._message, self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = self._message
        if len(self.args) > 0:
            # Convert all arguments into their string representations...
            args = ["%s" % arg for arg in self.args if arg]
            self._error_string = (self._error_string + "\nDetails: %s" % '\n'.join(args))
        return self._error_string.strip()

    def __str__(self):
        self.construct_error_string()
        return self._error_string.strip()

    def get_detail(self):
        self.construct_error_string()
        return self._error_string.strip() + "\nStacktrace: %s" % traceback.format_exc()


class NotImplementedException(IDDSException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(NotImplementedException, self).__init__(*args, **kwargs)
        self._message = "Not implemented exception."
        self.error_code = 101


class WrongParameterException(IDDSException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(WrongParameterException, self).__init__(*args, **kwargs)
        self._message = "Wrong parameter exception."
        self.error_code = 102


class DatabaseException(IDDSException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(DatabaseException, self).__init__(*args, **kwargs)
        self._message = "Database exception."
        self.error_code = 200


class InvalidDatabaseType(DatabaseException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(InvalidDatabaseType, self).__init__(*args, **kwargs)
        self._message = "Invalid Database type exception."
        self.error_code = 201


class NoObject(DatabaseException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(NoObject, self).__init__(*args, **kwargs)
        self._message = "No object  exception."
        self.error_code = 202


class DuplicatedObject(DatabaseException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(DuplicatedObject, self).__init__(*args, **kwargs)
        self._message = "Duplicated object exception."
        self.error_code = 203


class CoreException(IDDSException):
    """
    CoreException
    """
    def __init__(self, *args, **kwargs):
        super(CoreException, self).__init__(*args, **kwargs)
        self._message = "Core exception."
        self.error_code = 300


class ConflictRequestException(CoreException):
    """
    ConflictRequestException
    """
    def __init__(self, *args, **kwargs):
        super(ConflictRequestException, self).__init__(*args, **kwargs)
        self._message = "Conflict Reqeust exception."
        self.error_code = 301


class RestException(IDDSException):
    """
    RestException
    """
    def __init__(self, *args, **kwargs):
        super(RestException, self).__init__(*args, **kwargs)
        self._message = "Rest exception."
        self.error_code = 400


class BadRequest(RestException):
    """
    BadRequest
    """
    def __init__(self, *args, **kwargs):
        super(BadRequest, self).__init__(*args, **kwargs)
        self._message = "Bad request exception."
        self.error_code = 401


class ConnectionException(RestException):
    """
    ConnectionException
    """
    def __init__(self, *args, **kwargs):
        super(ConnectionException, self).__init__(*args, **kwargs)
        self._message = "Connection exception."
        self.error_code = 402


class ProcessNotFound(IDDSException):
    """
    ProcessNotFound
    """
    def __init__(self, *args, **kwargs):
        super(ProcessNotFound, self).__init__(*args, **kwargs)
        self._message = "Process not found."
        self.error_code = 403


class AgentException(IDDSException):
    """
    BrokerException
    """
    def __init__(self, *args, **kwargs):
        super(AgentException, self).__init__(*args, **kwargs)
        self._message = "Agent exception."
        self.error_code = 500


class NoPluginException(AgentException):
    """
    NoPluginException exception
    """
    def __init__(self, *args, **kwargs):
        super(NoPluginException, self).__init__(*args, **kwargs)
        self._message = "No plugin exception."
        self.error_code = 501


class AgentPluginError(AgentException):
    """
    AgentPluginError exception
    """
    def __init__(self, *args, **kwargs):
        super(AgentPluginError, self).__init__(*args, **kwargs)
        self._message = "Agent plugin exception."
        self.error_code = 502
