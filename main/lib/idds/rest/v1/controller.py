#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2024
# - Rudra Tiwari, <rudra.tiwari@student.unimelb.edu.au>, 2025


import time
from functools import wraps
from traceback import format_exc

from flask import Response, request
from flask.views import MethodView

from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import json_dumps, get_logger


def log_api_call(func):
    """
    Decorator to log REST API calls with timing and context information.
    This decorator automatically logs:
    - Request method and endpoint
    - Username (if authenticated)
    - Request duration
    - Response status code
    - Any exceptions that occur
    Usage:
        @log_api_call
        def get(self, request_id):
            ...
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        logger = self.get_logger()
        method_name = func.__name__.upper()
        endpoint = request.path
        username = self.get_username() or 'anonymous'

        # Log request start
        start_time = time.time()
        logger.info(
            "[%s] %s %s - user=%s - started",
            self.get_class_name(), method_name, endpoint, username
        )

        try:
            result = func(self, *args, **kwargs)
            duration = time.time() - start_time

            # Extract status code from response if possible
            status_code = getattr(result, 'status_code', 'unknown')
            logger.info(
                "[%s] %s %s - user=%s - completed - status=%s - duration=%.3fs",
                self.get_class_name(), method_name, endpoint, username,
                status_code, duration
            )
            return result

        except Exception as error:
            duration = time.time() - start_time
            logger.error(
                "[%s] %s %s - user=%s - failed - duration=%.3fs - error=%s",
                self.get_class_name(), method_name, endpoint, username,
                duration, str(error)
            )
            logger.error(format_exc())
            raise

    return wrapper


class IDDSController(MethodView):
    """ Default ESS Controller class. """

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self, logger=None):
        """
        Setup logger
        """
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(name=self.get_class_name(), filename='idds_rest.log')
        return self.logger

    def get_logger(self):
        if hasattr(self, 'logger') and self.logger:
            return self.logger
        return self.setup_logger()

    def log_request_context(self, message, level='info', **extra_context):
        """
        Log a message with request context information.
        This method provides a consistent way to log messages with relevant
        request context such as endpoint, username, and any additional context.
        Args:
            message: The log message
            level: Log level ('debug', 'info', 'warning', 'error')
            **extra_context: Additional context key-value pairs to include
        Usage:
            self.log_request_context("Processing request", request_id=123)
            self.log_request_context("Request failed", level='error', error=str(e))
        """
        logger = self.get_logger()
        username = self.get_username() or 'anonymous'
        endpoint = request.path
        method = request.method

        # Build context string from extra context
        context_parts = [f"{k}={v}" for k, v in extra_context.items()]
        context_str = " - ".join(context_parts) if context_parts else ""

        log_message = f"[{self.get_class_name()}] {method} {endpoint} - user={username}"
        if context_str:
            log_message += f" - {context_str}"
        log_message += f" - {message}"

        log_func = getattr(logger, level, logger.info)
        log_func(log_message)

    def post(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def get(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def put(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def delete(self):
        """ Not supported. """
        return Response(status=HTTP_STATUS_CODE.BadRequest, content_type='application/json')()

    def get_request(self):
        return request

    def get_username(self):
        if 'username' in request.environ and request.environ['username']:
            return request.environ['username']
        return None

    def generate_message(self, exc_cls=None, exc_msg=None):
        if exc_cls is None and exc_msg is None:
            return None
        else:
            message = {}
            # if exc_cls is not None:
            #     message['ExceptionClass'] = exc_cls
            if exc_msg is not None:
                message['msg'] = str(exc_msg)
            return json_dumps(message)

    def generate_http_response(self, status_code, data=None, exc_cls=None, exc_msg=None, log_error=True):
        """
        Generate an HTTP response with optional error logging.
        Args:
            status_code: HTTP status code
            data: Response data (will be JSON serialized)
            exc_cls: Exception class name for error responses
            exc_msg: Exception message for error responses
            log_error: If True, automatically log error responses (default: True)
        Returns:
            Flask Response object
        """
        # Automatically log error responses for debugging
        if log_error and exc_cls and status_code != HTTP_STATUS_CODE.OK:
            self.log_request_context(
                f"Error response: {exc_msg}",
                level='warning' if status_code == HTTP_STATUS_CODE.BadRequest else 'error',
                status_code=status_code,
                exception_class=exc_cls
            )

        enable_json_outputs = self.get_request().args.get('json_outputs', None)
        if enable_json_outputs and enable_json_outputs.upper() == 'TRUE':
            error = None
            if exc_cls:
                error = {'ExceptionClass': exc_cls,
                         'ExceptionMessage': self.generate_message(exc_cls, exc_msg)}
            if status_code == HTTP_STATUS_CODE.OK:
                status_code = 0
            response = {'status': status_code,
                        'data': data,
                        'error': error}
            resp = Response(response=json_dumps(response, sort_keys=True, indent=4), status=HTTP_STATUS_CODE.OK, content_type='application/json')
        else:
            resp = Response(response=json_dumps(data, sort_keys=True, indent=4) if data is not None else data, status=status_code, content_type='application/json')
            if exc_cls:
                resp.headers['ExceptionClass'] = exc_cls
                resp.headers['ExceptionMessage'] = self.generate_message(exc_cls, exc_msg)
        return resp
