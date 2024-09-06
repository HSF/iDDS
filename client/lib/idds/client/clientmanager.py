#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020 - 2024


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


from idds.common.authentication import OIDCAuthentication, OIDCAuthenticationUtils
from idds.common.utils import setup_logging, get_proxy_path, idds_mask

from idds.client.version import release_version
from idds.client.client import Client
from idds.common import exceptions
from idds.common.config import (get_local_cfg_file, get_local_config_root,
                                get_local_config_value, get_main_config_file)
from idds.common.constants import (WorkflowType, RequestType, RequestStatus,
                                   TransformType, TransformStatus)
# from idds.common.utils import get_rest_host, exception_handler
from idds.common.utils import exception_handler

# from idds.workflowv2.work import Work, Parameter, WorkStatus
# from idds.workflowv2.workflow import Condition, Workflow
from idds.workflowv2.work import Collection
from idds.workflow.work import Collection as CollectionV1


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
        self.x509_proxy = None
        self.oidc_token_file = None
        self.oidc_token = None
        self.vo = None

        self.enable_json_outputs = False

        self.configuration = ConfigParser.ConfigParser()

        self.client = None
        # if setup_client:
        #     self.setup_client()

    def setup_client(self, auth_setup=False):
        self.get_local_configuration()
        if self.host is None:
            local_cfg = self.get_local_cfg_file()
            self.host = self.get_config_value(local_cfg, 'rest', 'host', current=self.host, default=None)

        if self.client is None:
            if self.auth_type is None:
                self.auth_type = 'x509_proxy'
            self.client = Client(host=self.host,
                                 auth={'auth_type': self.auth_type,
                                       'client_proxy': self.x509_proxy,
                                       'oidc_token_file': self.oidc_token_file,
                                       'oidc_token': self.oidc_token,
                                       'vo': self.vo,
                                       'auth_setup': auth_setup},
                                 timeout=self.timeout)

            if self.enable_json_outputs:
                self.client.enable_json_outputs()

    def setup_json_outputs(self):
        self.enable_json_outputs = True
        if self.client:
            self.client.enable_json_outputs()

    def get_local_config_root(self):
        local_cfg_root = get_local_config_root(self.local_config_root)
        return local_cfg_root

    def get_local_cfg_file(self):
        local_cfg = get_local_cfg_file(self.local_config_root)
        return local_cfg

    def get_config_value(self, configuration, section, name, current, default):
        name_envs = {'host': 'IDDS_HOST',
                     'local_config_root': 'IDDS_LOCAL_CONFIG_ROOT',
                     'config': 'IDDS_CONFIG',
                     'auth_type': 'IDDS_AUTH_TYPE',
                     'oidc_token_file': 'IDDS_OIDC_TOKEN_FILE',
                     'oidc_token': 'IDDS_OIDC_TOKEN',
                     'vo': 'IDDS_VO',
                     'auth_no_verify': 'IDDS_AUTH_NO_VERIFY',
                     'enable_json_outputs': 'IDDS_ENABLE_JSON_OUTPUTS'}

        additional_name_envs = {'oidc_token': 'OIDC_AUTH_ID_TOKEN',
                                'oidc_token_file': 'OIDC_AUTH_TOKEN_FILE',
                                'vo': 'OIDC_AUTH_VO'}
        if not section:
            section = self.get_section(name)

        if name in name_envs:
            env_value = os.environ.get(name_envs[name], None)
            if env_value and len(env_value.strip()) > 0:
                return env_value
        if name in additional_name_envs:
            env_value = os.environ.get(additional_name_envs[name], None)
            if env_value and len(env_value.strip()) > 0:
                return env_value

        if configuration and type(configuration) in [str]:
            config = ConfigParser.ConfigParser()
            config.read(configuration)
            configuration = config

        value = get_local_config_value(configuration, section, name, current, default)
        return value

    def get_section(self, name):
        name_sections = {'config': 'common',
                         'auth_type': 'common',
                         'host': 'rest',
                         'x509_proxy': 'x509_proxy',
                         'oidc_token_file': 'oidc',
                         'oidc_token': 'oidc',
                         'vo': 'oidc'}
        if name in name_sections:
            return name_sections[name]
        return 'common'

    def get_local_configuration(self):
        local_cfg = self.get_local_cfg_file()
        main_cfg = get_main_config_file()
        config = ConfigParser.ConfigParser()
        if local_cfg and os.path.exists(local_cfg) and main_cfg:
            config.read((main_cfg, local_cfg))
        else:
            if main_cfg:
                config.read(main_cfg)
            elif local_cfg and os.path.exists(local_cfg):
                config.read(local_cfg)
            else:
                logging.debug("No local configuration nor IDDS_CONFIG, will only load idds default value.")

        if self.get_local_config_root():
            self.config = self.get_config_value(config, section=None, name='config', current=self.config,
                                                default=os.path.join(self.get_local_config_root(), 'idds.cfg'))
        else:
            self.config = self.get_config_value(config, section=None, name='config', current=self.config,
                                                default=None)

        self.auth_type = self.get_config_value(config, None, 'auth_type', current=self.auth_type, default='x509_proxy')

        self.host = self.get_config_value(config, None, 'host', current=self.host, default=None)

        self.x509_proxy = self.get_config_value(config, None, 'x509_proxy', current=self.x509_proxy,
                                                default='/tmp/x509up_u%d' % os.geteuid())
        if 'X509_USER_PROXY' in os.environ or not self.x509_proxy or not os.path.exists(self.x509_proxy):
            proxy = get_proxy_path()
            if proxy:
                self.x509_proxy = proxy

        if self.get_local_config_root():
            self.oidc_token_file = self.get_config_value(config, None, 'oidc_token_file', current=self.oidc_token_file,
                                                         default=os.path.join(self.get_local_config_root(), '.token'))
            self.oidc_token = self.get_config_value(config, None, 'oidc_token', current=self.oidc_token,
                                                    default=None)
        else:
            self.oidc_token_file = self.get_config_value(config, None, 'oidc_token_file', current=self.oidc_token_file,
                                                         default=None)
            self.oidc_token = self.get_config_value(config, None, 'oidc_token', current=self.oidc_token,
                                                    default=None)

        self.vo = self.get_config_value(config, None, 'vo', current=self.vo, default=None)

        self.enable_json_outputs = self.get_config_value(config, None, 'enable_json_outputs',
                                                         current=self.enable_json_outputs, default=None)
        self.configuration = config

    def set_local_configuration(self, name, value):
        if value:
            section = self.get_section(name)
            if self.configuration and not self.configuration.has_section(section):
                self.configuration.add_section(section)
            if name in ['oidc_refresh_lifetime']:
                value = str(value)
            elif name in ['oidc_auto', 'oidc_polling']:
                value = str(value).lower()
            if self.configuration:
                self.configuration.set(section, name, value)

    def save_local_configuration(self):
        local_cfg = self.get_local_cfg_file()
        if not local_cfg:
            logging.debug("local configuration file does not exist, will not store current setup.")
        else:
            self.set_local_configuration(name='config', value=self.config)
            self.set_local_configuration(name='auth_type', value=self.auth_type)
            self.set_local_configuration(name='host', value=self.host)
            self.set_local_configuration(name='x509_proxy', value=self.x509_proxy)
            self.set_local_configuration(name='oidc_token_file', value=self.oidc_token_file)
            self.set_local_configuration(name='oidc_token', value=self.oidc_token)
            self.set_local_configuration(name='vo', value=self.vo)
            self.set_local_configuration(name='enable_json_outputs', value=self.enable_json_outputs)

            with open(local_cfg, 'w') as configfile:
                self.configuration.write(configfile)

    def setup_local_configuration(self, local_config_root=None, config=None, host=None,
                                  auth_type=None, x509_proxy=None, oidc_token=None,
                                  oidc_token_file=None, vo=None):

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
        self.x509_proxy = x509_proxy
        self.oidc_token_file = oidc_token_file
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
            status, output = oidc_util.save_token(self.oidc_token_file, token)
            if status:
                logging.info("Token is saved to %s" % (self.oidc_token_file))
            else:
                logging.info("Failed to save token to %s: (status: %s, output: %s)" % (self.oidc_token_file, status, output))

    def setup_oidc_client_token(self, issuer, client_id, client_secret, scope, audience):
        """"
        Setup oidc client token
        """
        self.setup_client(auth_setup=True)
        oidc_auth = OIDCAuthentication()
        status, token = oidc_auth.setup_oidc_client_token(issuer=issuer, client_id=client_id,
                                                          client_secret=client_secret, scope=scope,
                                                          audience=audience)

        if not status:
            logging.error("Failed to get token.")
        else:
            oidc_util = OIDCAuthenticationUtils()
            token_file = self.oidc_token_file + "_client"
            status, output = oidc_util.save_token(token_file, token)
            if status:
                logging.info("Token is saved to %s" % (token_file))
            else:
                logging.info("Failed to save token to %s: (status: %s, output: %s)" % (token_file, status, output))

    def refresh_oidc_token(self):
        """"
        refresh oidc token
        """
        self.setup_client(auth_setup=True)

        oidc_util = OIDCAuthenticationUtils()
        status, token = oidc_util.load_token(self.oidc_token_file)
        if not status:
            logging.error("Token %s cannot be loaded: %s" % (status, token))
            return

        is_expired, output = oidc_util.is_token_expired(token)
        if is_expired:
            logging.error("Token %s is already expired(%s). Cannot refresh." % self.oidc_token_file, output)
        else:
            new_token = self.client.refresh_id_token(self.vo, token['refresh_token'])
            status, data = oidc_util.save_token(self.oidc_token_file, new_token)
            if status:
                logging.info("New token saved to %s" % self.oidc_token_file)
            else:
                logging.info("Failed to save token to %s: %s" % (self.oidc_token_file, data))

    @exception_handler
    def clean_oidc_token(self):
        """"
        Clean oidc token
        """
        self.setup_client(auth_setup=True)

        oidc_util = OIDCAuthenticationUtils()
        status, output = oidc_util.clean_token(self.oidc_token_file)
        if status:
            logging.info("Token %s is cleaned" % self.oidc_token_file)
        else:
            logging.error("Failed to clean token %s: status: %s, output: %s" % (self.oidc_token_file, status, output))

    @exception_handler
    def check_oidc_token_status(self):
        """"
        Check oidc token status
        """
        self.setup_client(auth_setup=True)

        oidc_util = OIDCAuthenticationUtils()
        if self.oidc_token:
            token = self.oidc_token
        else:
            status, token = oidc_util.load_token(self.oidc_token_file)
            if not status:
                logging.error("Token %s cannot be loaded: status: %s, error: %s" % (self.oidc_token_file, status, token))
                return
            token = token['id_token']

        status, token_info = oidc_util.get_token_info(token)
        if status:
            if self.oidc_token:
                logging.info("ID token: %s" % self.oidc_token)
            else:
                logging.info("Token path: %s" % self.oidc_token_file)
            for k in token_info:
                logging.info("Token %s: %s" % (k, token_info[k]))
        else:
            logging.error("Failed to parse token information: %s" % str(token_info))

    def set_original_user(self, user_name=None, user_dn=None, user_cert=None, user_token=None):
        """
        Set original user
        """
        self.setup_client()
        self.client.set_original_user(user_name=user_name,
                                      user_dn=user_dn,
                                      user_cert=user_cert,
                                      user_token=user_token)

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
    def submit(self, workflow, username=None, userdn=None, use_dataset_name=False):
        """
        Submit the workflow as a request to iDDS server.

        :param workflow: The workflow to be submitted.
        """
        self.setup_client()

        scope = 'workflow'
        request_type = RequestType.Workflow
        transform_tag = 'workflow'
        priority = 0
        try:
            if workflow.workflow_type in [WorkflowType.iWorkflow]:
                scope = 'iworkflow'
                request_type = RequestType.iWorkflow
                transform_tag = workflow.get_work_tag()
                priority = workflow.priority
                if priority is None:
                    priority = 0
            elif workflow.workflow_type in [WorkflowType.iWorkflowLocal]:
                scope = 'iworkflowLocal'
                request_type = RequestType.iWorkflowLocal
                transform_tag = workflow.get_work_tag()
                priority = workflow.priority
                if priority is None:
                    priority = 0
        except Exception:
            pass

        props = {
            'scope': scope,
            'name': workflow.name,
            'requester': 'panda',
            'request_type': request_type,
            'username': username if username else workflow.username,
            'userdn': userdn if userdn else workflow.userdn,
            'transform_tag': transform_tag,
            'status': RequestStatus.New,
            'priority': priority,
            'site': workflow.get_site(),
            'lifetime': workflow.lifetime,
            'workload_id': workflow.get_workload_id(),
            'request_metadata': {'version': release_version, 'workload_id': workflow.get_workload_id(), 'workflow': workflow}
        }

        if self.client.original_user_name:
            props['username'] = self.client.original_user_name
        if self.client.original_user_dn:
            props['userdn'] = self.client.original_user_dn

        if self.auth_type == 'x509_proxy':
            if hasattr(workflow, 'add_proxy'):
                workflow.add_proxy()

        if use_dataset_name or not workflow.name:
            primary_init_work = workflow.get_primary_initial_collection()
            if primary_init_work:
                if type(primary_init_work) in [Collection, CollectionV1]:
                    props['scope'] = primary_init_work.scope
                    props['name'] = primary_init_work.name
                else:
                    props['scope'] = primary_init_work['scope']
                    props['name'] = primary_init_work['name']

        # print(props)
        request_id = self.client.add_request(**props)
        return request_id

    @exception_handler
    def submit_work(self, request_id, work, use_dataset_name=False):
        """
        Submit the workflow as a request to iDDS server.

        :param workflow: The workflow to be submitted.
        """
        self.setup_client()

        transform_type = TransformType.Workflow
        transform_tag = 'work'
        priority = 0
        workload_id = None
        try:
            if work.workflow_type in [WorkflowType.iWork]:
                transform_type = TransformType.iWork
                transform_tag = work.get_work_tag()
                workload_id = work.workload_id
                priority = work.priority
                if priority is None:
                    priority = 0
            elif work.workflow_type in [WorkflowType.iWorkflow, WorkflowType.iWorkflowLocal]:
                transform_type = TransformType.iWorkflow
                transform_tag = work.get_work_tag()
                workload_id = work.workload_id
                priority = work.priority
                if priority is None:
                    priority = 0
        except Exception:
            pass

        props = {
            'workload_id': workload_id,
            'transform_type': transform_type,
            'transform_tag': transform_tag,
            'priority': work.priority,
            'retries': 0,
            'parent_transform_id': None,
            'previous_transform_id': None,
            # 'site': work.site,
            'name': work.name,
            'token': work.token,
            'status': TransformStatus.New,
            'transform_metadata': {'version': release_version, 'work': work}
        }

        # print(props)
        transform_id = self.client.add_transform(request_id, **props)
        return transform_id

    @exception_handler
    def submit_build(self, workflow, username=None, userdn=None, use_dataset_name=True):
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
            'request_metadata': {'version': release_version, 'workload_id': workflow.get_workload_id(), 'build_workflow': workflow}
        }

        if self.client.original_user_name:
            props['username'] = self.client.original_user_name
        if self.client.original_user_dn:
            props['userdn'] = self.client.original_user_dn

        if self.auth_type == 'x509_proxy':
            workflow.add_proxy()

        if use_dataset_name:
            primary_init_work = workflow.get_primary_initial_collection()
            if primary_init_work:
                if type(primary_init_work) in [Collection, CollectionV1]:
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

        ret = self.client.abort_request(request_id=request_id, workload_id=workload_id)
        # return (-1, 'No matching requests')
        return ret

    @exception_handler
    def abort_task(self, request_id=None, workload_id=None, task_id=None):
        """
        Abort task.

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

        ret = self.client.abort_request_task(request_id=request_id, workload_id=workload_id, task_id=task_id)
        return ret

    @exception_handler
    def close(self, request_id=None, workload_id=None):
        """
        Close requests.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        self.setup_client()

        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        ret = self.client.close_request(request_id=request_id, workload_id=workload_id)
        # return (-1, 'No matching requests')
        return ret

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

        ret = self.client.retry_request(request_id=request_id, workload_id=workload_id)
        return ret

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
    def get_request_id_by_name(self, name):
        """
        Get request id by name.

        :param name: the request name.

        :returns {name:id} dict.
        """
        self.setup_client()

        ret = self.client.get_request_id_by_name(name=name)
        return ret

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
    def get_transforms(self, request_id=None, workload_id=None):
        """
        Get transforms.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        self.setup_client()

        tfs = self.client.get_transforms(request_id=request_id, workload_id=workload_id)
        return tfs

    @exception_handler
    def get_transform(self, request_id=None, transform_id=None):
        """
        Get transforms.

        :param transform_id: the transform id.
        :param request_id: the request.
        """
        self.setup_client()

        tf = self.client.get_transform(request_id=request_id, transform_id=transform_id)
        return tf

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
            return (True, "Logs are downloaded to %s" % filename)
        else:
            logging.info("Failed to download logs for workload_id(%s) and request_id(%s)" % (workload_id, request_id))
            return (False, "Failed to download logs for workload_id(%s) and request_id(%s)" % (workload_id, request_id))

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
    def send_messages(self, request_id=None, workload_id=None, transform_id=None, internal_id=None, msgs=None):
        """
        Send messages.

        :param workload_id: the workload id.
        :param request_id: the request.
        """
        self.setup_client()

        if request_id is None and workload_id is None:
            logging.error("Both request_id and workload_id are None. One of them should not be None")
            return (-1, "Both request_id and workload_id are None. One of them should not be None")

        logging.info("Retrieving messages for request_id: %s, workload_id: %s" % (request_id, workload_id))
        self.client.send_messages(request_id=request_id, workload_id=workload_id, transform_id=transform_id, internal_id=internal_id, msgs=msgs)
        logging.info("Sent %s messages for request_id: %s, workload_id: %s" % (len(msgs), request_id, workload_id))
        return True, None

    @exception_handler
    def get_messages(self, request_id=None, workload_id=None, transform_id=None, internal_id=None):
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
        msgs = self.client.get_messages(request_id=request_id, workload_id=workload_id, transform_id=transform_id, internal_id=internal_id)
        logging.info("Retrieved %s messages for request_id: %s, workload_id: %s" % (len(msgs), request_id, workload_id))
        return (True, msgs)

    @exception_handler
    def get_collections(self, request_id=None, transform_id=None, workload_id=None, scope=None, name=None, relation_type=None):
        """
        Get collections from the Head service.

        :param request_id: the request id.
        :param transform_id: the transform id.
        :param workload_id: the workload id.
        :param scope: the scope.
        :param name: the name.
        :param relation_type: the relation type (input, output and log).

        :raise exceptions if it's not got successfully.
        """
        self.setup_client()

        colls = self.client.get_collections(request_id=request_id, transform_id=transform_id, workload_id=workload_id,
                                            scope=scope, name=name, relation_type=relation_type)
        return colls

    def get_contents(self, request_id=None, transform_id=None, workload_id=None, coll_scope=None, coll_name=None, relation_type=None, status=None):
        """
        Get contents from the Head service.

        :param request_id: the request id.
        :param transform_id: the transform id.
        :param workload_id: the workload id.
        :param coll_scope: the scope of the related collection.
        :param coll_name: the name of the related collection.
        :param relation_type: the relation type (input, output and log).
        :param status: the status of related contents.

        :raise exceptions if it's not got successfully.
        """
        self.setup_client()

        contents = self.client.get_contents(request_id=request_id, transform_id=transform_id, workload_id=workload_id,
                                            coll_scope=coll_scope, coll_name=coll_name, relation_type=relation_type, status=status)
        return contents

    @exception_handler
    def get_contents_output_ext(self, request_id=None, workload_id=None, transform_id=None, group_by_jedi_task_id=False):
        """
        Get output extension contents from the Head service.

        :param request_id: the request id.
        :param workload_id: the workload id.
        :param transform_id: the transform id.

        :raise exceptions if it's not got successfully.
        """
        self.setup_client()

        contents = self.client.get_contents_output_ext(workload_id=workload_id, request_id=request_id, transform_id=transform_id,
                                                       group_by_jedi_task_id=group_by_jedi_task_id)
        return contents

    @exception_handler
    def update_build_request(self, request_id, signature, workflow):
        """
        Update Build Request to the Head service.

        :param request_id: the request.
        :param signature: the signature of the request.
        :param workflow: the workflow of the request.

        :raise exceptions if it's not updated successfully.
        """
        self.setup_client()

        ret = self.client.update_build_request(request_id=request_id, signature=signature, workflow=workflow)
        return ret

    @exception_handler
    def get_metainfo(self, name):
        """
        Get meta info.

        :param name: the name of the meta info.
        """
        self.setup_client()

        logging.info("Retrieving meta info for %s" % (name))
        ret = self.client.get_metainfo(name=name)
        logging.info("Retrieved meta info for %s: %s" % (name, idds_mask(ret)))
        return True, ret
