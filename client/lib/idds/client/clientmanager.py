#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2021


"""
Workflow manager.
"""
import logging
import tabulate

from idds.common.utils import setup_logging

from idds.client.version import release_version
from idds.client.client import Client
from idds.common.constants import RequestType, RequestStatus
from idds.common.utils import get_rest_host, exception_handler

# from idds.workflow.work import Work, Parameter, WorkStatus
# from idds.workflow.workflow import Condition, Workflow
from idds.workflow.work import Collection


setup_logging(__name__)


class ClientManager:
    def __init__(self, host=None):
        self.host = host
        if self.host is None:
            self.host = get_rest_host()
        self.client = Client(host=self.host)

    @exception_handler
    def submit(self, workflow):
        """
        Submit the workflow as a request to iDDS server.

        :param workflow: The workflow to be submitted.
        """
        props = {
            'scope': 'workflow',
            'name': workflow.name,
            'requester': 'panda',
            'request_type': RequestType.Workflow,
            'transform_tag': 'workflow',
            'status': RequestStatus.New,
            'priority': 0,
            'lifetime': workflow.lifetime,
            'workload_id': workflow.get_workload_id(),
            'request_metadata': {'version': release_version, 'workload_id': workflow.get_workload_id(), 'workflow': workflow}
        }
        workflow.add_proxy()
        primary_init_work = workflow.get_primary_initial_collection()
        if primary_init_work:
            if type(primary_init_work) in [Collection]:
                props['scope'] = primary_init_work.scope
                props['name'] = primary_init_work.name
            else:
                props['scope'] = primary_init_work['scope']
                props['name'] = primary_init_work['name']

        # print(props)
        request_id = self.client.add_request(**props)
        return request_id

    @exception_handler
    def abort(self, request_id=None, workload_id=None):
        """
        Abort requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id)
        if reqs:
            rets = []
            for req in reqs:
                logging.info("Aborting request: %s" % req['request_id'])
                # self.client.update_request(request_id=req['request_id'], parameters={'substatus': RequestStatus.ToCancel})
                self.client.send_message(request_id=req['request_id'], msg={'command': 'update_request', 'parameters': {'status': RequestStatus.ToCancel}})
                logging.info("Abort request registered successfully: %s" % req['request_id'])
                ret = (0, "Abort request registered successfully: %s" % req['request_id'])
                rets.append(ret)
            return rets
        else:
            return (-1, 'No matching requests')

    @exception_handler
    def suspend(self, request_id=None, workload_id=None):
        """
        Suspend requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id)
        if reqs:
            rets = []
            for req in reqs:
                logging.info("Suspending request: %s" % req['request_id'])
                # self.client.update_request(request_id=req['request_id'], parameters={'substatus': RequestStatus.ToSuspend})
                self.client.send_message(request_id=req['request_id'], msg={'command': 'update_request', 'parameters': {'status': RequestStatus.ToSuspend}})
                logging.info("Suspend request registered successfully: %s" % req['request_id'])
                ret = (0, "Suspend request registered successfully: %s" % req['request_id'])
                rets.append(ret)
            return rets
        else:
            return (-1, 'No matching requests')

    @exception_handler
    def resume(self, request_id=None, workload_id=None):
        """
        Resume requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id)
        if reqs:
            rets = []
            for req in reqs:
                logging.info("Resuming request: %s" % req['request_id'])
                # self.client.update_request(request_id=req['request_id'], parameters={'substatus': RequestStatus.ToResume})
                self.client.send_message(request_id=req['request_id'], msg={'command': 'update_request', 'parameters': {'status': RequestStatus.ToResume}})
                logging.info("Resume request registered successfully: %s" % req['request_id'])
                ret = (0, "Resume request registered successfully: %s" % req['request_id'])
                rets.append(ret)
            return rets
        else:
            return (-1, 'No matching requests')

    @exception_handler
    def retry(self, request_id=None, workload_id=None):
        """
        Retry requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id)
        if reqs:
            rets = []
            for req in reqs:
                logging.info("Retrying request: %s" % req['request_id'])
                # self.client.update_request(request_id=req['request_id'], parameters={'substatus': RequestStatus.ToResume})
                self.client.send_message(request_id=req['request_id'], msg={'command': 'update_request', 'parameters': {'status': RequestStatus.ToResume}})
                logging.info("Retry request registered successfully: %s" % req['request_id'])
                ret = (0, "Retry request registered successfully: %s" % req['request_id'])
                rets.append(ret)
            return rets
        else:
            return (-1, 'No matching requests')

    @exception_handler
    def finish(self, request_id=None, workload_id=None, set_all_finished=False):
        """
        Retry requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id)
        if reqs:
            rets = []
            for req in reqs:
                logging.info("Finishing request: %s" % req['request_id'])
                if set_all_finished:
                    # self.client.update_request(request_id=req['request_id'], parameters={'substatus': RequestStatus.ToForceFinish})
                    self.client.send_message(request_id=req['request_id'], msg={'command': 'update_request', 'parameters': {'status': RequestStatus.ToForceFinish}})
                else:
                    # self.client.update_request(request_id=req['request_id'], parameters={'substatus': RequestStatus.ToFinish})
                    self.client.send_message(request_id=req['request_id'], msg={'command': 'update_request', 'parameters': {'status': RequestStatus.ToFinish}})
                logging.info("ToFinish request registered successfully: %s" % req['request_id'])
                ret = (0, "ToFinish request registered successfully: %s" % req['request_id'])
                rets.append(ret)
            return rets
        else:
            return (-1, 'No matching requests')

    @exception_handler
    def get_requests(self, request_id=None, workload_id=None, with_detail=False, with_metadata=False):
        """
        Get requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        :param with_detail: Whether to show detail info.
        """
        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id, with_detail=with_detail, with_metadata=with_metadata)
        return reqs

    @exception_handler
    def get_status(self, request_id=None, workload_id=None, with_detail=False, with_metadata=False):
        """
        Get the status progress report of requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        :param with_detail: Whether to show detail info.
        """
        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id, with_detail=with_detail, with_metadata=with_metadata)
        if with_detail:
            table = []
            for req in reqs:
                table.append([req['request_id'], req['transform_id'], req['workload_id'], req['transform_workload_id'], "%s:%s" % (req['output_coll_scope'], req['output_coll_name']),
                              "%s[%s/%s/%s]" % (req['transform_status'].name, req['output_total_files'], req['output_processed_files'], req['output_processing_files']),
                              req['errors']])
            ret = tabulate.tabulate(table, tablefmt='simple', headers=['request_id', 'transform_id', 'request_workload_id', 'transform_workload_id', 'scope:name', 'status[Total/OK/Processing]', 'errors'])
            # print(ret)
            return str(ret)
        else:
            table = []
            for req in reqs:
                table.append([req['request_id'], req['workload_id'], "%s:%s" % (req['scope'], req['name']), req['status'].name, req['errors']])
            ret = tabulate.tabulate(table, tablefmt='simple', headers=['request_id', 'request_workload_id', 'scope:name', 'status', 'errors'])
            # print(ret)
            return str(ret)

    @exception_handler
    def download_logs(self, request_id=None, workload_id=None, dest_dir='./', filename=None):
        """
        Download logs for a request.

        :param workload_id: the workload id.
        :param request_id: the request.
        :param dest_dir: The destination directory.
        :param filename: The destination filename to be saved. If it's None, default filename will be saved.
        """
        filename = self.client.download_logs(request_id=request_id, workload_id=workload_id, dest_dir=dest_dir, filename=filename)
        if filename:
            logging.info("Logs are downloaded to %s" % filename)
            return (0, "Logs are downloaded to %s" % filename)
        else:
            logging.info("Failed to download logs for workload_id(%s) and request_id(%s)" % (workload_id, request_id))
            return (-1, "Failed to download logs for workload_id(%s) and request_id(%s)" % (workload_id, request_id))

    @exception_handler
    def upload_to_cacher(self, filename):
        """
        Upload file to iDDS cacher: On the cacher, the filename will be the basename of the file.
        """
        return self.client.upload(filename)

    @exception_handler
    def download_from_cacher(self, filename):
        """
        Download file from iDDS cacher: On the cacher, the filename will be the basename of the file.
        """
        return self.client.download(filename)

    @exception_handler
    def get_hyperparameters(self, workload_id, request_id, id=None, status=None, limit=None):
        """
        Get hyperparameters from the Head service.

        :param workload_id: the workload id.
        :param request_id: the request id.
        :param status: the status of the hyperparameters.
        :param limit: limit number of hyperparameters

        :raise exceptions if it's not got successfully.
        """
        return self.client.get_hyperparameters(workload_id=workload_id, request_id=request_id, id=id, status=status, limit=limit)

    @exception_handler
    def update_hyperparameter(self, workload_id, request_id, id, loss):
        """
        Update hyperparameter to the Head service.

        :param workload_id: the workload id.
        :param request_id: the request.
        :param id: id of the hyper parameter.
        :param loss: the loss.

        :raise exceptions if it's not updated successfully.
        """
        return self.client.update_hyperparameter(workload_id=workload_id, request_id=request_id, id=id, loss=loss)

    @exception_handler
    def get_messages(self, request_id=None, workload_id=None):
        """
        Get messages.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        logging.info("Retrieving messages for request_id: %s, workload_id: %s" % (request_id, workload_id))
        msgs = self.client.get_messages(request_id=request_id, workload_id=workload_id)
        logging.info("Retrieved %s messages for request_id: %s, workload_id: %s" % (len(msgs), request_id, workload_id))
        return (0, msgs)
