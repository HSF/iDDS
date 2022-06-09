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
import datetime
import os
import sys
import logging
import tabulate
import time

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

try:
    from urllib import urlencode        # noqa F401
except ImportError:
    from urllib.parse import urlencode  # noqa F401
    raw_input = input


from idds.common.authentication import OIDCAuthenticationUtils
from idds.common.utils import setup_logging, get_proxy_path

from idds.client.version import release_version
from idds.client.client import Client
from idds.common import exceptions
from idds.common.config import get_local_cfg_file, get_local_config_root, get_local_config_value
from idds.common.constants import RequestType, RequestStatus, ProcessingStatus
# from idds.common.utils import get_rest_host, exception_handler
from idds.common.utils import exception_handler

# from idds.workflowv2.work import Work, Parameter, WorkStatus
# from idds.workflowv2.workflow import Condition, Workflow
from idds.workflowv2.work import Collection


setup_logging(__name__)


class ClientManager:
    def __init__(self, host=None, timeout=600, setup_client=False):
        self.host = host
        self.timeout = timeout
        # if self.host is None:
        #     self.host = get_rest_host()
        # self.client = Client(host=self.host)

        self.local_config_root = None
        self.config = None
        self.auth_type = None
        self.auth_type_host = None
        self.x509_proxy = None
        self.oidc_token = None
        self.vo = None

        self.configuration = ConfigParser.SafeConfigParser()

        self.client = None
        # if setup_client:
        #     self.setup_client()

    def setup_client(self, auth_setup=False):
        self.setup_local_configuration(host=self.host)
        if self.host is None:
            local_cfg = self.get_local_cfg_file()
            if self.auth_type is None:
                self.auth_type = 'x509_proxy'
            self.host = self.get_config_value(local_cfg, self.auth_type, 'host', current=self.host, default=None)
            if self.host is None:
                self.host = self.get_config_value(local_cfg, 'rest', 'host', current=self.host, default=None)

        if self.client is None:
            if self.auth_type_host is not None:
                client_host = self.auth_type_host
            else:
                client_host = self.host
            if self.auth_type is None:
                self.auth_type = 'x509_proxy'
            self.client = Client(host=client_host,
                                 auth={'auth_type': self.auth_type,
                                       'client_proxy': self.x509_proxy,
                                       'oidc_token': self.oidc_token,
                                       'vo': self.vo,
                                       'auth_setup': auth_setup},
                                 timeout=self.timeout)

    def get_local_config_root(self):
        local_cfg_root = get_local_config_root(self.local_config_root)
        return local_cfg_root

    def get_local_cfg_file(self):
        local_cfg = get_local_cfg_file(self.local_config_root)
        return local_cfg

    def get_config_value(self, configuration, section, name, current, default):
        if configuration and type(configuration) in [str]:
            config = ConfigParser.SafeConfigParser()
            config.read(configuration)
            configuration = config
        value = get_local_config_value(configuration, section, name, current, default)
        return value

    def get_local_configuration(self):
        local_cfg = self.get_local_cfg_file()
        config = ConfigParser.SafeConfigParser()
        if not local_cfg:
            logging.debug("local configuration file does not exist, will only load idds default value.")
        if local_cfg and os.path.exists(local_cfg):
            config.read(local_cfg)

        if self.get_local_config_root():
            self.config = self.get_config_value(config, section='common', name='config', current=self.config,
                                                default=os.path.join(self.get_local_config_root(), 'idds.cfg'))
        else:
            self.config = self.get_config_value(config, section='common', name='config', current=self.config,
                                                default=None)

        self.auth_type = self.get_config_value(config, 'common', 'auth_type', current=self.auth_type, default='x509_proxy')

        self.host = self.get_config_value(config, 'rest', 'host', current=self.host, default=None)
        self.auth_type_host = self.get_config_value(config, self.auth_type, 'host', current=self.auth_type_host, default=None)

        self.x509_proxy = self.get_config_value(config, 'x509_proxy', 'x509_proxy', current=self.x509_proxy,
                                                default='/tmp/x509up_u%d' % os.geteuid())
        if not self.x509_proxy or not os.path.exists(self.x509_proxy):
            proxy = get_proxy_path()
            if proxy:
                self.x509_proxy = proxy

        if self.get_local_config_root():
            self.oidc_token = self.get_config_value(config, 'oidc', 'oidc_token', current=self.oidc_token,
                                                    default=os.path.join(self.get_local_config_root(), '.oidc_token'))
        else:
            self.oidc_token = self.get_config_value(config, 'oidc', 'oidc_token', current=self.oidc_token,
                                                    default=None)

        self.vo = self.get_config_value(config, self.auth_type, 'vo', current=self.vo, default=None)

        self.configuration = config

    def save_local_configuration(self):
        local_cfg = self.get_local_cfg_file()
        if not local_cfg:
            logging.debug("local configuration file does not exist, will not store current setup.")
        else:
            with open(local_cfg, 'w') as configfile:
                self.configuration.write(configfile)

    def setup_local_configuration(self, local_config_root=None, config=None, host=None,
                                  auth_type=None, auth_type_host=None, x509_proxy=None,
                                  oidc_token=None, vo=None):

        if 'IDDS_CONFIG' in os.environ and os.environ['IDDS_CONFIG']:
            if config is None:
                print("IDDS_CONFIG is set. Will use it.")
                config = os.environ['IDDS_CONFIG']
            else:
                print("config is set to %s. Ignore IDDS_CONFIG" % config)

        self.local_config_root = local_config_root
        self.config = config
        self.host = host
        self.auth_type = auth_type
        self.auth_type_host = auth_type_host
        self.x509_proxy = x509_proxy
        self.oidc_token = oidc_token
        self.vo = vo

        self.get_local_configuration()
        self.save_local_configuration()

    def setup_oidc_token(self):
        """"
        Setup oidc token
        """
        self.setup_client(auth_setup=True)

        sign_url = self.client.get_oidc_sign_url(self.vo)
        logging.info(("Please go to {0} and sign in. "
                      "Waiting until authentication is completed").format(sign_url['verification_uri_complete']))

        logging.info('Ready to get ID token?')
        while True:
            sys.stdout.write("[y/n] \n")
            choice = raw_input().lower()
            if choice == 'y':
                break
            elif choice == 'n':
                logging.info('aborted')
                return

        if 'interval' in sign_url:
            interval = sign_url['interval']
        else:
            interval = 5

        if 'expires_in' in sign_url:
            expires_in = sign_url['expires_in']
        else:
            expires_in = 60

        token = None
        count = 0
        start_time = datetime.datetime.utcnow()
        while datetime.datetime.utcnow() - start_time < datetime.timedelta(seconds=expires_in):
            try:
                output = self.client.get_id_token(self.vo, sign_url['device_code'])
                logging.debug("get_id_token: %s" % (output))
                token = output
                break
            except exceptions.AuthenticationPending as error:
                logging.debug("Authentication pending: %s" % str(error))
                time.sleep(interval)
                count += 1
                # if count % 5 == 0:
                logging.info("Authentication is still pending. Please follow the link to authorize it.")
            except Exception as error:
                logging.error("get_id_token: exception: %s" % str(error))
                break

        if not token:
            logging.error("Failed to get token.")
        else:
            oidc_util = OIDCAuthenticationUtils()
            status, output = oidc_util.save_token(self.oidc_token, token)
            if status:
                logging.info("Token is saved to %s" % (self.oidc_token))
            else:
                logging.info("Failed to save token to %s: (status: %s, output: %s)" % (self.oidc_token, status, output))

    def refresh_oidc_token(self):
        """"
        refresh oidc token
        """
        self.setup_client(auth_setup=True)

        oidc_util = OIDCAuthenticationUtils()
        status, token = oidc_util.load_token(self.oidc_token)
        if not status:
            logging.error("Token %s cannot be loaded: %s" % (status, token))
            return

        is_expired, output = oidc_util.is_token_expired(token)
        if is_expired:
            logging.error("Token %s is already expired(%s). Cannot refresh." % self.oidc_token, output)
        else:
            new_token = self.client.refresh_id_token(self.vo, token['refresh_token'])
            status, data = oidc_util.save_token(self.oidc_token, new_token)
            if status:
                logging.info("New token saved to %s" % self.oidc_token)
            else:
                logging.info("Failed to save token to %s: %s" % (self.oidc_token, data))

    @exception_handler
    def clean_oidc_token(self):
        """"
        Clean oidc token
        """
        self.setup_client(auth_setup=True)

        oidc_util = OIDCAuthenticationUtils()
        status, output = oidc_util.clean_token(self.oidc_token)
        if status:
            logging.info("Token %s is cleaned" % self.oidc_token)
        else:
            logging.error("Failed to clean token %s: status: %s, output: %s" % (self.oidc_token, status, output))

    @exception_handler
    def check_oidc_token_status(self):
        """"
        Check oidc token status
        """
        self.setup_client(auth_setup=True)

        oidc_util = OIDCAuthenticationUtils()
        status, token = oidc_util.load_token(self.oidc_token)
        if not status:
            logging.error("Token %s cannot be loaded: status: %s, error: %s" % (self.oidc_token, status, token))
            return

        status, token_info = oidc_util.get_token_info(token)
        if status:
            logging.info("Token path: %s" % self.oidc_token)
            for k in token_info:
                logging.info("Token %s: %s" % (k, token_info[k]))
        else:
            logging.error("Failed to parse token information: %s" % str(token_info))

    @exception_handler
    def ping(self):
        """
        Ping the idds server
        """
        self.setup_client()
        status = self.client.ping()
        # logging.info("Ping idds server: %s" % str(status))
        return status

    @exception_handler
    def submit(self, workflow, username=None, userdn=None, use_dataset_name=True):
        """
        Submit the workflow as a request to iDDS server.

        :param workflow: The workflow to be submitted.
        """
        self.setup_client()

        props = {
            'scope': 'workflow',
            'name': workflow.name,
            'requester': 'panda',
            'request_type': RequestType.Workflow,
            'username': username if username else workflow.username,
            'userdn': userdn if userdn else workflow.userdn,
            'transform_tag': 'workflow',
            'status': RequestStatus.New,
            'priority': 0,
            'lifetime': workflow.lifetime,
            'workload_id': workflow.get_workload_id(),
            'request_metadata': {'version': release_version, 'workload_id': workflow.get_workload_id(), 'workflow': workflow}
        }

        if self.auth_type == 'x509_proxy':
            workflow.add_proxy()

        if use_dataset_name:
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
        self.setup_client()

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
    def abort_tasks(self, request_id=None, workload_id=None, task_id=None):
        """
        Abort tasks.

        :param workload_id: the workload id.
        :param request_id: the request.
        :param task_id: The task id.
        """
        self.setup_client()

        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")
        if task_id is None:
            logging.error("The task_id is required for killing tasks. If you want to kill the whole workflow, please try another API.")
            return (-1, "The task_id is required for killing tasks")

        reqs = self.client.get_requests(request_id=request_id, workload_id=workload_id, with_processing=True)
        if reqs:
            rets = []
            for req in reqs:
                if str(req['processing_workload_id']) == str(task_id):
                    logging.info("Aborting task: (request_id: %s, task_id: %s)" % (req['request_id'], task_id))
                    self.client.send_message(request_id=req['request_id'], msg={'command': 'update_processing',
                                                                                'parameters': [{'status': ProcessingStatus.ToCancel, 'workload_id': task_id}]})
                    logging.info("Abort task registered successfully: (request_id %s, task_id: %s)" % (req['request_id'], task_id))
                    ret = (0, "Abort task registered successfully: (request_id %s, task_id: %s)" % (req['request_id'], task_id))
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
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

        return self.client.upload(filename)

    @exception_handler
    def download_from_cacher(self, filename):
        """
        Download file from iDDS cacher: On the cacher, the filename will be the basename of the file.
        """
        self.setup_client()

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
        self.setup_client()

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
        self.setup_client()

        return self.client.update_hyperparameter(workload_id=workload_id, request_id=request_id, id=id, loss=loss)

    @exception_handler
    def get_messages(self, request_id=None, workload_id=None):
        """
        Get messages.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        self.setup_client()

        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        logging.info("Retrieving messages for request_id: %s, workload_id: %s" % (request_id, workload_id))
        msgs = self.client.get_messages(request_id=request_id, workload_id=workload_id)
        logging.info("Retrieved %s messages for request_id: %s, workload_id: %s" % (len(msgs), request_id, workload_id))
        return (0, msgs)
