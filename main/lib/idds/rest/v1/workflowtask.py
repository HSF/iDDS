#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025

"""
REST endpoints for workflow task management.

POST /workflow_task
    Create a workflow task.
    Body: {"workflow": <workflow-dict>}

PUT  /workflow_task/<request_id>/<transform_id>/<workload_id>/adjust
    Adjust worker resource parameters.
    Body: {"parameters": {"core_count": .., "memory_per_core": .., "site": ..}}

PUT  /workflow_task/<request_id>/close
PUT  /workflow_task/<request_id>/<workload_id>/close   (legacy)
    Close a workflow task.
    Body: {"parameters": <content-dict>}
"""

from traceback import format_exc

from flask import Blueprint

from idds.common import exceptions
from idds.common.constants import HTTP_STATUS_CODE
from idds.common.utils import json_loads, setup_logging
from idds.rest.v1.controller import IDDSController

from idds.prompt.handlers.workflowtaskhandler import (  # type: ignore[import-untyped]
    create_workflow_task as _create_workflow_task,
    adjust_worker as _adjust_worker,
    close_workflow_task as _close_workflow_task,
)


setup_logging(__name__)


class WorkflowTaskCreate(IDDSController):
    """
    Create a workflow task.

    POST /workflow_task

    Body (new format matching the spec's ``idds_create_workflow_task`` API):
    {
        "workflow": {
            "scope":          "<scope>",
            "name":           "<name>",
            "requester":      "<requester>",
            "username":       "<username>",
            "transform_tag":  "<tag>",
            "cloud":          "<cloud>",
            "campaign":       "<campaign>",
            "campaign_scope": "<campaign_scope>",
            "campaign_group": "<campaign_group>",
            "campaign_tag":   "<campaign_tag>",
            "content": {
                "run_id":          "<run_id>",
                "core_count":      <int>,
                "memory_per_core": <float>,
                "site":            "<site>",
                "panda_attributes": { ... },
                ...
            }
        }
    }

    Returns:
    {
        "request_id":    <int>,
        "transform_id":  <int>,
        "processing_id": <int>,
        "input_coll_id":  <int>,
        "output_coll_id": <int>,
        "workload_id":   <int|null>
    }
    """

    def post(self):
        """Create workflow task."""
        logger = self.get_logger()
        try:
            body = self.get_request().data and json_loads(self.get_request().data)
            if not body:
                raise exceptions.BadRequest("Request body must not be empty")

            workflow = body.get('workflow')
            if not workflow:
                raise exceptions.BadRequest("'workflow' key is required in the request body")

        except exceptions.BadRequest as error:
            return self.generate_http_response(
                HTTP_STATUS_CODE.BadRequest,
                exc_cls=error.__class__.__name__,
                exc_msg=str(error),
            )
        except Exception as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.BadRequest,
                exc_cls=exceptions.BadRequest.__name__,
                exc_msg="Cannot decode JSON parameter dictionary",
            )

        try:
            (
                request_id,
                transform_id,
                processing_id,
                input_coll_id,
                output_coll_id,
                workload_id,
            ) = _create_workflow_task(workflow, logger=logger)

        except exceptions.IDDSException as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.InternalError,
                exc_cls=error.__class__.__name__,
                exc_msg=str(error),
            )
        except Exception as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.InternalError,
                exc_cls=exceptions.CoreException.__name__,
                exc_msg=str(error),
            )

        return self.generate_http_response(
            HTTP_STATUS_CODE.OK,
            data={
                'request_id': request_id,
                'transform_id': transform_id,
                'processing_id': processing_id,
                'input_coll_id': input_coll_id,
                'output_coll_id': output_coll_id,
                'workload_id': workload_id,
            },
        )


class WorkflowTaskAdjust(IDDSController):
    """
    Adjust worker resource parameters for an existing workflow task.

    PUT /workflow_task/<request_id>/<transform_id>/<workload_id>/adjust

    Body (matching the spec's ``idds_adjust_worker`` API):
    {
        "parameters": {
            "core_count":      <int>,
            "memory_per_core": <float>,
            "site":            "<site>",
            "content":         { ... }
        }
    }

    Returns: {"status": 0, "message": "adjusted successfully"}
    """

    def put(self, request_id, transform_id=None, workload_id=None):
        """Adjust worker parameters."""
        logger = self.get_logger()

        if request_id == 'null':
            request_id = None
        if transform_id == 'null':
            transform_id = None
        if workload_id == 'null':
            workload_id = None

        try:
            body = {}
            if self.get_request().data:
                body = json_loads(self.get_request().data) or {}
            parameters = body.get('parameters', {})
        except Exception as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.BadRequest,
                exc_cls=exceptions.BadRequest.__name__,
                exc_msg="Cannot decode JSON parameter dictionary",
            )

        try:
            _adjust_worker(request_id, transform_id, workload_id, parameters, logger=logger)
        except exceptions.IDDSException as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.InternalError,
                exc_cls=error.__class__.__name__,
                exc_msg=str(error),
            )
        except Exception as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.InternalError,
                exc_cls=exceptions.CoreException.__name__,
                exc_msg=str(error),
            )

        return self.generate_http_response(
            HTTP_STATUS_CODE.OK,
            data={'status': 0, 'message': 'adjusted successfully'},
        )


class WorkflowTaskClose(IDDSController):
    """
    Close a workflow task.

    PUT /workflow_task/<request_id>/close
    PUT /workflow_task/<request_id>/<workload_id>/close  (legacy)

    Body (matching the spec's ``idds_close_workflow_task`` API):
    {
        "parameters": {
            "request_id":   <int>,
            "transform_id": <int>,
            "workload_id":  <int>,
            ...
        }
    }

    Returns: {"status": 0, "message": "closed successfully"}
    """

    def put(self, request_id, workload_id=None):
        """Close workflow task."""
        logger = self.get_logger()

        if request_id == 'null':
            request_id = None
        if workload_id == 'null':
            workload_id = None

        try:
            body = {}
            if self.get_request().data:
                body = json_loads(self.get_request().data) or {}
            parameters = body.get('parameters', {})
            # fall back: if workload_id given in URL, ensure it's in parameters
            if workload_id and not parameters.get('workload_id'):
                parameters = dict(parameters)
                parameters['workload_id'] = workload_id
        except Exception as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.BadRequest,
                exc_cls=exceptions.BadRequest.__name__,
                exc_msg="Cannot decode JSON parameter dictionary",
            )

        try:
            _close_workflow_task(request_id, parameters=parameters, logger=logger)
        except exceptions.IDDSException as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.InternalError,
                exc_cls=error.__class__.__name__,
                exc_msg=str(error),
            )
        except Exception as error:
            logger.error(error)
            logger.error(format_exc())
            return self.generate_http_response(
                HTTP_STATUS_CODE.InternalError,
                exc_cls=exceptions.CoreException.__name__,
                exc_msg=str(error),
            )

        return self.generate_http_response(
            HTTP_STATUS_CODE.OK,
            data={'status': 0, 'message': 'closed successfully'},
        )


# ---------------------------------------------------------------------------
# Blueprint
# ---------------------------------------------------------------------------

def get_blueprint():
    bp = Blueprint('workflowtask', __name__)

    wft_create = WorkflowTaskCreate.as_view('workflow_task_create')
    bp.add_url_rule('/workflow_task', view_func=wft_create, methods=['post'])

    wft_adjust = WorkflowTaskAdjust.as_view('workflow_task_adjust')
    bp.add_url_rule(
        '/workflow_task/<request_id>/<transform_id>/<workload_id>/adjust',
        view_func=wft_adjust,
        methods=['put'],
    )

    wft_close = WorkflowTaskClose.as_view('workflow_task_close')
    bp.add_url_rule(
        '/workflow_task/<request_id>/close',
        view_func=wft_close,
        methods=['put'],
    )
    bp.add_url_rule(
        '/workflow_task/<request_id>/<workload_id>/close',
        view_func=wft_close,
        methods=['put'],
    )

    return bp
