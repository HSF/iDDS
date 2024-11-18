#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2024

import atexit
import logging
import os
import random
import socket
import threading
import time
import traceback
import uuid

from queue import Queue

try:
    import stomp
    with_stomp = True
except Exception as ex:
    print(f"Failed to import stomp, with_stomp is False: {ex}")
    with_stomp = False

from idds.common.constants import WorkflowType, GracefulEvent
from idds.common.utils import json_dumps, json_loads, setup_logging, get_unique_id_for_dict, timeout_wrapper, is_panda_client_verbose
from .base import Base


setup_logging(__name__)
logging.getLogger("stomp").setLevel(logging.CRITICAL)


class MessagingListener(stomp.ConnectionListener):
    '''
    Messaging Listener
    '''
    def __init__(self, broker, output_queue, logger=None):
        '''
        __init__
        '''
        self.name = "MessagingListener"
        self.__broker = broker
        self.__output_queue = output_queue
        # self.logger = logging.getLogger(self.__class__.__name__)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, frame):
        '''
        Error handler
        '''
        self.logger.error('[broker] [%s]: %s', self.__broker, frame.body)

    def on_message(self, frame):
        self.logger.debug('[broker] [%s]: headers: %s, body: %s', self.__broker, frame.headers, frame.body)
        self.__output_queue.put(json_loads(frame.body))


class MapResult(object):
    def __init__(self):
        self._name_results = {}
        self._results = {}

    def __str__(self):
        return str(self._name_results)

    def add_result(self, name=None, args=None, key=None, result=None):
        name_key = key
        if name_key is None:
            key = get_unique_id_for_dict(args)
            name_key = '%s:%s' % (name, key)
        else:
            # name_key = key
            # name = ':'.join(name_key.split(":")[:-1])
            key = name_key.split(":")[-1]

        self._name_results[name_key] = result
        self._results[key] = result

    def has_result(self, name=None, args=None, key=None):
        name_key = key
        if name_key is not None:
            if name_key in self._name_results:
                return True
            return False
        else:
            key = get_unique_id_for_dict(args)
            name_key = '%s:%s' % (name, key)

            if name is not None:
                if name_key in self._name_results:
                    return True
                return False
            else:
                if key in self._result:
                    return True
                return False

    def get_result(self, name=None, args=None, key=None, verbose=False):
        if verbose:
            logging.info("get_result: key %s, name: %s, args: %s" % (key, name, args))
            logging.info("get_result: results: %s, name_results: %s" % (self._results, self._name_results))

        name_key = key
        if name_key is not None:
            ret = self._name_results.get(name_key, None)
        else:
            key = get_unique_id_for_dict(args)

            if name is not None:
                name_key = '%s:%s' % (name, key)
                ret = self._name_results.get(name_key, None)
            else:
                ret = self._results.get(key, None)
        if verbose:
            logging.info("get_result: name key %s, args key %s, ret: %s" % (name_key, key, ret))
        return ret

    def set_result(self, name=None, args=None, key=None, value=None, verbose=False):
        if verbose:
            logging.info("set_result: key %s, name: %s, args: %s, value: %s" % (key, name, args, value))
            logging.info("set_result: results: %s, name_results: %s" % (self._results, self._name_results))

        name_key = key
        if name_key is not None:
            self._name_results[name_key] = value
        else:
            key = get_unique_id_for_dict(args)

            if name is not None:
                name_key = '%s:%s' % (name, key)
                self._name_results[name_key] = value
            else:
                self._results[key] = value
        if verbose:
            logging.info("set_result: name key %s, args key %s, value: %s" % (name_key, key, value))

    def get_all_results(self):
        return self._results

    def get_dict_results(self):
        return {'results': self._results, 'name_results': self._name_results}

    def set_from_dict_results(self, results):
        self._results = results.get('results', {})
        self._name_results = results.get('name_results', {})


class AsyncResult(Base):

    def __init__(self, work_context, name=None, wait_num=1, wait_keys=[], multi_jobs_kwargs_list=[], current_job_kwargs=None, map_results=False,
                 wait_percent=1, internal_id=None, timeout=None):
        """
        Init a workflow.
        """
        super(AsyncResult, self).__init__()
        if internal_id:
            self.internal_id = internal_id
        self._work_context = work_context
        try:
            ret = self._work_context.init_brokers()
            if ret:
                self._broker_initialized = True
            else:
                self._broker_initialized = False
        except Exception as ex:
            logging.warn(f"{self.internal_id} Failed to initialize messaging broker, will use Rest: {ex}")
            self._broker_initialized = False

        self._name = name
        self._queue = Queue()

        self._connections = []
        self._subscribe_connections = []
        self._graceful_stop = False
        self._is_stop = False
        self._subscribe_thread = None
        self._subscribed = False

        self._results = []
        self._bad_results = []
        self._results_percentage = 0
        self._map_results = map_results
        self.waiting_result_terminated = False

        self._wait_num = wait_num
        if not self._wait_num:
            self._wait_num = 1
        self._wait_keys = set(wait_keys)
        self._multi_jobs_kwargs_list = multi_jobs_kwargs_list
        self._current_job_kwargs = current_job_kwargs

        self._wait_percent = wait_percent
        self._num_wrong_keys = 0

        self._timeout = timeout

        self._nologs = False

        self._num_stomp_failures = 0
        self._max_stomp_failures = 5
        try:
            max_stomp_failures = os.environ.get("AYNC_RESULT_MAX_STOMP_FAILURES", None)
            if max_stomp_failures:
                max_stomp_failures = int(max_stomp_failures)
                self._max_stomp_failures = max_stomp_failures
        except Exception:
            pass

        self._poll_period = 300
        try:
            poll_period = os.environ.get("AYNC_RESULT_POLL_PERIOD", None)
            if poll_period:
                poll_period = int(poll_period)
                self._poll_period = poll_period
        except Exception:
            pass

        self._is_messaging_ok = True
        self._is_polling_ok = True

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    @logger.setter
    def logger(self, value):
        pass

    @property
    def wait_keys(self):
        if len(self._wait_keys) > 0:
            self._wait_num = len(self._wait_keys)
            return self._wait_keys
        if self._multi_jobs_kwargs_list:
            for kwargs in self._multi_jobs_kwargs_list:
                k = get_unique_id_for_dict(kwargs)
                k = "%s:%s" % (self._name, k)
                request_id, transform_id, internal_id = self.get_request_id_internal_id()
                self.logger.info(f"request_id {request_id} transform_id {transform_id} internal_id {internal_id} args ({kwargs}) to key: {k}")
                self._wait_keys.add(k)
            self._wait_num = len(self._wait_keys)
        return self._wait_keys

    @wait_keys.setter
    def wait_keys(self, value):
        self._wait_keys = set(value)

    @property
    def is_all_results_available(self):
        percent = self.get_results_percentage()
        if percent >= self._wait_percent:
            return True

    @is_all_results_available.setter
    def is_all_results_available(self, value):
        raise Exception(f"{self.internal_id} Not allowd to set is_all_results_available")

    @property
    def is_stop(self):
        return self._is_stop

    @is_stop.setter
    def is_stop(self, value):
        raise Exception(f"{self.internal_id} Not allowd to set is_stop")

    @property
    def is_finished(self):
        if self._graceful_stop and self._graceful_stop.is_set():
            percent = self.get_results_percentage()
            if percent >= self._wait_percent:
                return True
        return False

    @is_finished.setter
    def is_finished(self, value):
        raise Exception(f"{self.internal_id} Not allowd to set is_finished")

    @property
    def is_subfinished(self):
        if self._graceful_stop and self._graceful_stop.is_set():
            percent = self.get_results_percentage()
            if percent > 0 and percent < self._wait_percent:
                return True
        return False

    @is_subfinished.setter
    def is_subfinished(self, value):
        raise Exception(f"{self.internal_id} Not allowd to set is_subfinished")

    @property
    def is_failed(self):
        if self._graceful_stop and self._graceful_stop.is_set():
            percent = self.get_results_percentage()
            if percent <= 0:
                return True
        return False

    @is_failed.setter
    def is_failed(self, value):
        raise Exception(f"{self.internal_id} Not allowd to set is_failed")

    @property
    def is_terminated(self):
        return self._graceful_stop and self._graceful_stop.is_set()

    @is_terminated.setter
    def is_terminated(self, value):
        raise Exception(f"{self.internal_id} Not allowd to set is_terminated")

    @property
    def results(self):
        has_new_data = False
        while not self._queue.empty():
            ret = self._queue.get()
            has_new_data = True
            try:
                internal_id = ret['internal_id']
                if internal_id == self.internal_id:
                    self._results.append(ret)
                else:
                    self._bad_results.append(ret)
            except Exception as ex:
                self.logger.error(f"{self.internal_id} Received bad result: {ret}: {ex}")
        if self._bad_results:
            self.logger.error(f"{self.internal_id} Received bad results: {self._bad_results}")

        if not self._nologs:
            self.logger.debug(f"{self.internal_id} _results: {self._results}, bad_results: {self._bad_results}")
            self.logger.debug(f"{self.internal_id} wait_keys: {self.wait_keys}, wait_num: {self._wait_num}")

        rets_dict = {}
        failure_dict = {}
        for result in self._results:
            key = result['key']
            ret = result['ret']
            status, output, error = ret
            if status is True:
                rets_dict[key] = output
            else:
                if key not in failure_dict:
                    failure_dict[key] = []
                failure_dict[key].append(ret)
        if not self._nologs:
            self.logger.debug(f"{self.internal_id} rets_dict: {rets_dict}, failure_dict: {failure_dict}")

        if self._map_results:
            rets = {}
            if len(self.wait_keys) > 0:
                for k in self.wait_keys:
                    if k in rets_dict:
                        rets[k] = rets_dict[k]
                self._results_percentage = len(list(rets.keys())) * 1.0 / len(self.wait_keys)
            else:
                rets = rets_dict
                self._results_percentage = len(list(rets.keys())) * 1.0 / self._wait_num

            ret_map = MapResult()
            for k in rets:
                ret_map.add_result(key=k, result=rets[k])

            if has_new_data:
                self.logger.debug(f'{self.internal_id} percent {self._results_percentage}, results: {ret_map}')

            return ret_map
        else:
            rets = []
            if len(self.wait_keys) > 0:
                for k in self.wait_keys:
                    if k in rets_dict:
                        rets.append(rets_dict[k])
                self._results_percentage = len(rets) * 1.0 / len(self.wait_keys)
            else:
                rets = [rets_dict[k] for k in rets_dict]
                self._results_percentage = len(rets) * 1.0 / self._wait_num

            if has_new_data:
                self.logger.debug(f'{self.internal_id} percent {self._results_percentage}, results: {rets}')

            if self._wait_num == 1:
                if rets:
                    return rets[0]
                else:
                    return None
            return rets

    @results.setter
    def results(self, value):
        raise Exception(f"{self.internal_id} Not allowed to set results.")
        if type(value) not in [list, tuple]:
            raise Exception(f"{self.internal_id} Results must be list or tuple, currently it is {value}")
        self._results = value

    def disconnect(self):
        for con in self._connections:
            try:
                if con.is_connected():
                    con.disconnect()
            except Exception:
                pass
        self._connections = []

    def disconnect_subscribe(self):
        for con in self._subscribe_connections:
            try:
                if con.is_connected():
                    con.disconnect()
            except Exception:
                pass
        self._subscribe_connections = []

    def has_connections(self, conns):
        if conns:
            for con in conns:
                try:
                    if con.is_connected():
                        return True
                except Exception:
                    pass
        return False

    def get_connections(self, conns):
        if conns:
            for con in conns:
                try:
                    if con.is_connected():
                        return con
                except Exception:
                    pass
        return None

    def connect_to_messaging_broker(self):
        conn = self.get_connections(self._connections)
        if conn:
            return conn

        workflow_context = self._work_context
        brokers = workflow_context.brokers

        brokers = brokers.split(",")
        broker = random.sample(brokers, k=1)[0]

        self.logger.info("Got broker: %s" % (broker))

        timeout = workflow_context.broker_timeout
        self.disconnect()

        broker, port = broker.split(":")
        conn = stomp.Connection12(host_and_ports=[(broker, port)],
                                  keepalive=True,
                                  heartbeats=(30000, 30000),     # half minute = num / 1000
                                  timeout=timeout)
        conn.connect(workflow_context.broker_username, workflow_context.broker_password, wait=True)
        self._connections = [conn]
        return conn

    def subscribe_to_messaging_brokers(self, force=False):
        if self._subscribed and not force and self._subscribe_connections:
            return self._subscribe_connections

        workflow_context = self._work_context
        brokers = workflow_context.brokers
        conns = []

        broker_addresses = []
        if not brokers:
            raise Exception(f"brokers <{brokers}> not defined")
        else:
            for b in brokers.split(","):
                try:
                    b, port = b.split(":")

                    addrinfos = socket.getaddrinfo(b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
                    for addrinfo in addrinfos:
                        b_addr = addrinfo[4][0]
                        broker_addresses.append((b_addr, port))
                except socket.gaierror as error:
                    self.logger.error(f'{self.internal_id} Cannot resolve hostname {b}: {error}')
                    # self._graceful_stop.set()

        self.logger.info(f"{self.internal_id} Resolved broker addresses: {broker_addresses}")

        timeout = workflow_context.broker_timeout

        self.disconnect_subscribe()

        listener = MessagingListener(brokers, self._queue, logger=self.logger)
        conns = []
        for broker, port in broker_addresses:
            conn = stomp.Connection12(host_and_ports=[(broker, port)],
                                      keepalive=True,
                                      heartbeats=(30000, 30000),     # half minute = num / 1000
                                      timeout=timeout)
            conn.set_listener("messag-subscriber", listener)
            conn.connect(workflow_context.broker_username, workflow_context.broker_password, wait=True)
            if workflow_context.workflow_type in [WorkflowType.iWorkflow, WorkflowType.iWorkflowLocal]:
                subscribe_id = 'idds-workflow_%s' % self.internal_id
                # subscribe_selector = {'selector': "type = 'iworkflow' AND request_id = %s" % workflow_context.request_id}
                # subscribe_selector = {'selector': "type = 'iworkflow' AND internal_id = '%s'" % self.internal_id}
                subscribe_selector = {'selector': "internal_id = '%s'" % self.internal_id}
            elif workflow_context.workflow_type == WorkflowType.iWork:
                subscribe_id = 'idds-work_%s' % self.internal_id
                # subscribe_selector = {'selector': "type = 'iwork' AND request_id = %s AND transform_id = %s " % (workflow_context.request_id,
                #                                                                                                  workflow_context.transform_id)}
                # subscribe_selector = {'selector': "type = 'iwork' AND internal_id = '%s'" % self.internal_id}
                subscribe_selector = {'selector': "internal_id = '%s'" % self.internal_id}
            else:
                subscribe_id = 'idds-workflow_%s' % self.internal_id
                subscribe_selector = None
            # subscribe_selector = None
            # conn.subscribe(destination=workflow_context.broker_destination, id=subscribe_id,
            #                ack='auto', conf=subscribe_selector)
            conn.subscribe(destination=workflow_context.broker_destination, id=subscribe_id,
                           ack='auto', headers=subscribe_selector)
            self.logger.info(f"{self.internal_id} subscribe to {broker}:{port} with selector: {subscribe_selector}")
            conns.append(conn)
        self._subscribe_connections = conns
        return conns

    def get_message(self, ret, key=None):
        message = {}
        workflow_context = self._work_context
        if key is None:
            if self._current_job_kwargs:
                key = get_unique_id_for_dict(self._current_job_kwargs)
                key = "%s:%s" % (self._name, key)
                self.logger.info(f"{self.internal_id} publish args ({self._current_job_kwargs}) to key: {key}")

        if workflow_context.workflow_type in [WorkflowType.iWorkflow, WorkflowType.iWorkflowLocal]:
            headers = {'persistent': 'true',
                       'channel': 'asyncresult',
                       'type': 'iworkflow',
                       'internal_id': str(self.internal_id),
                       'request_id': workflow_context.request_id}
            body = {'ret': ret, 'key': key, 'internal_id': self.internal_id, 'type': 'iworkflow',
                    'request_id': workflow_context.request_id}
            message = {"headers": headers, "body": body}
        elif workflow_context.workflow_type == WorkflowType.iWork:
            headers = {'persistent': 'true',
                       'channel': 'asyncresult',
                       'type': 'iwork',
                       'internal_id': str(self.internal_id),
                       'request_id': workflow_context.request_id,
                       'transform_id': workflow_context.transform_id}
            body = {'ret': ret, 'key': key, 'internal_id': self.internal_id, 'type': 'iwork',
                    'request_id': workflow_context.request_id,
                    'transform_id': workflow_context.transform_id}
            message = {"headers": headers, "body": body}
        return message

    def publish_message(self, ret, key=None):
        message = self.get_message(ret=ret, key=key)
        headers = message['headers']
        body = message['body']
        conn = self.connect_to_messaging_broker()
        workflow_context = self._work_context
        if workflow_context.workflow_type in [WorkflowType.iWorkflow, WorkflowType.iWorkflowLocal]:
            conn.send(body=json_dumps(body),
                      destination=workflow_context.broker_destination,
                      id='idds-iworkflow_%s' % self.internal_id,
                      ack='auto',
                      headers=headers
                      )
            self.logger.info(f"{self.internal_id} published header: {headers}, body: {body}")
        elif workflow_context.workflow_type == WorkflowType.iWork:
            conn.send(body=json_dumps(body),
                      destination=workflow_context.broker_destination,
                      id='idds-iwork_%s' % self.internal_id,
                      ack='auto',
                      headers=headers
                      )
            self.logger.info(f"{self.internal_id} published header: {headers}, body: {body}")
        # self.disconnect()

    def get_request_id_internal_id(self):
        workflow_context = self._work_context
        request_id, transform_id, internal_id = None, None, None
        if workflow_context.workflow_type in [WorkflowType.iWorkflow, WorkflowType.iWorkflowLocal]:
            request_id = workflow_context.request_id
            transform_id = 0
            # internal_id = workflow_context.internal_id
        elif workflow_context.workflow_type == WorkflowType.iWork:
            request_id = workflow_context.request_id
            transform_id = workflow_context.transform_id
            # internal_id = workflow_context.internal_id
        else:
            request_id = workflow_context.request_id
            transform_id = 0
            # internal_id = workflow_context.internal_id
        internal_id = self.internal_id
        return request_id, transform_id, internal_id

    def publish_through_panda_server(self, request_id, transform_id, internal_id, message):
        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api
        idds_server = self._work_context.get_idds_server()
        # request_id = self._context.request_id
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)
        status, ret = client.send_messages(request_id=request_id, transform_id=transform_id, internal_id=internal_id, msgs=[message])
        if status == 0 and type(ret) in (list, tuple) and len(ret) > 1 and ret[0] is True and type(ret[1]) in (list, tuple) and ret[1][0] is True:
            self.logger.info(f"{self.internal_id} published message through panda server: {message}")
        else:
            self.logger.error(f"{self.internal_id} failed to publish message through panda server, status: {status}, ret: {ret}")

    def publish_through_idds_server(self, request_id, transform_id, internal_id, message):
        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._work_context.get_idds_server(), timeout=60)
        status, ret = client.send_messages(request_id=request_id, transform_id=transform_id, internal_id=internal_id, msgs=[message])
        if status:
            self.logger.info(f"{self.internal_id} published message through idds server: {message}")
        else:
            self.logger.error(f"{self.internal_id} failed to publish message through idds server, status: {status}, ret: {ret}")

    def publish_through_api(self, ret, key=None, force=False):
        message = self.get_message(ret=ret, key=key)
        # headers = message['headers']
        # body = message['body']
        message['msg_type'] = 'async_result'

        try:
            request_id, transform_id, internal_id = self.get_request_id_internal_id()
            if request_id is None:
                if force:
                    request_id = 0
                else:
                    self.logger.warn(f"{self.internal_id} Not to publish message through API since the request id is None")
                    return

            if self._work_context.service == 'panda':
                self.publish_through_panda_server(request_id, transform_id, internal_id, message)
            else:
                self.publish_through_idds_server(request_id, transform_id, internal_id, message)
        except Exception as ex:
            self.logger.error(f"{self.internal_id} Failed to publish message through API: {ex}")

    @timeout_wrapper(timeout=90)
    def publish(self, ret, key=None, force=False, ret_status=True, ret_error=None):
        stomp_failed = False
        if with_stomp and self._broker_initialized:
            try:
                self.logger.info(f"{self.internal_id} publishing results through messaging brokers")
                self.publish_message(ret=(ret_status, ret, ret_error), key=key)
                self.logger.info(f"{self.internal_id} finished to publish results through messaging brokers")
            except Exception as ex:
                self.logger.warn(f"{self.internal_id} Failed to publish result through messaging brokers: {ex}")
                stomp_failed = True

        if not with_stomp or not self._broker_initialized or stomp_failed:
            self.logger.info(f"{self.internal_id} publishing results through http API")
            self.publish_through_api(ret=(ret_status, ret, ret_error), key=key, force=force)
            self.logger.info(f"{self.internal_id} finished to publish results through http API")

    def poll_messages_through_panda_server(self, request_id, transform_id, internal_id):
        if request_id is None:
            self.logger.warn(f"{self.internal_id} Not to poll message through panda server, since the request_id is None")
            return []

        import idds.common.utils as idds_utils
        import pandaclient.idds_api as idds_api
        idds_server = self._work_context.get_idds_server()
        # request_id = self._work_context.request_id
        client = idds_api.get_api(idds_utils.json_dumps,
                                  idds_host=idds_server,
                                  compress=True,
                                  verbose=is_panda_client_verbose(),
                                  manager=True)
        status, ret = client.get_messages(request_id=request_id, transform_id=transform_id, internal_id=internal_id)
        if status == 0 and type(ret) in (list, tuple) and len(ret) > 1 and ret[0] is True and type(ret[1]) in (list, tuple) and ret[1][0] is True:
            self.logger.info(f"{self.internal_id} poll message through panda server, ret: {ret}")
            messages = ret[1][1]
            self.logger.info(f"{self.internal_id} poll message through panda server, number of messages: {len(messages)}")
            return messages
        else:
            self.logger.error(f"{self.internal_id} failed to poll messages through panda server, status: {status}, ret: {ret}")
            return []

    def poll_messages_through_idds_server(self, request_id, transform_id, internal_id):
        if request_id is None:
            self.logger.warn(f"{self.internal_id} Not to poll message through idds server, since the request_id is None")
            return []

        from idds.client.clientmanager import ClientManager
        client = ClientManager(host=self._work_context.get_idds_server(), timeout=60)
        status, messages = client.get_messages(request_id=request_id, transform_id=transform_id, internal_id=internal_id)
        if status:
            self.logger.info(f"{self.internal_id} poll message through panda server, ret: {messages}")
            self.logger.info(f"{self.internal_id} poll message through idds server, number of messages: {len(messages)}")
            return messages
        else:
            self.logger.error(f"{self.internal_id} failed to poll messages through idds server, error: {messages}")
            return []

    @timeout_wrapper(timeout=90)
    def poll_messages(self, force=False):
        try:
            request_id, transform_id, internal_id = self.get_request_id_internal_id()
            if request_id is None:
                if force:
                    request_id = 0
                else:
                    self.logger.warn(f"{self.internal_id} Not to poll message, since the request_id is None")
                    return

            if self._work_context.service == 'panda':
                messages = self.poll_messages_through_panda_server(request_id=request_id, transform_id=transform_id, internal_id=internal_id)
            else:
                messages = self.poll_messages_through_idds_server(request_id=request_id, transform_id=transform_id, internal_id=internal_id)

            for message in messages:
                body = message['body']
                self._queue.put(body)
        except Exception as ex:
            self.logger.error(f"{self.internal_id} Failed to poll message: {ex}")

    def run_subscriber(self, force=False):
        try:
            self.logger.info(f"{self.internal_id} run subscriber")
            if with_stomp and self._broker_initialized and self._num_stomp_failures < self._max_stomp_failures:
                try:
                    self.subscribe_to_messaging_brokers(force=True)
                except Exception as ex:
                    self.logger.warn(f"{self.internal_id} run subscriber fails to subscribe to message broker: {ex}")
                    self._num_stomp_failures += 1
                    self._is_messaging_ok = False
                    self._broker_initialized = False

            time_poll = None
            time_start = time.time()
            while self._graceful_stop and not self._graceful_stop.is_set():
                if with_stomp and self._broker_initialized and self._is_messaging_ok and self._num_stomp_failures < self._max_stomp_failures:
                    has_failed_conns = False
                    for conn in self._subscribe_connections:
                        if not conn.is_connected():
                            has_failed_conns = True
                    if has_failed_conns:
                        try:
                            self.subscribe_to_messaging_brokers(force=True)
                        except Exception as ex:
                            self.logger.warn(f"{self.internal_id} run subscriber fails to subscribe to message broker: {ex}")
                            self._num_stomp_failures += 1
                            self._is_messaging_ok = False
                            self._broker_initialized = False
                    time.sleep(1)
                else:
                    if self._timeout:
                        sleep_time = min(self._timeout / 3, self._poll_period)
                    else:
                        sleep_time = self._poll_period

                    if time_poll is None or time.time() - time_poll > sleep_time:
                        try:
                            self.poll_messages(force=force)
                        except Exception as ex:
                            self.logger.info(f"{self.internal_id} run subscriber fails to poll messages: {ex}")
                        time_poll = time.time()
                    time.sleep(1)

                if self._timeout and time.time() - time_start > self._timeout:
                    self.logger.info(f"{self.internal_id} timeout reached")
                    break

            if self._graceful_stop and self._graceful_stop.is_set():
                self.logger.info(f"{self.internal_id} graceful stop is set")

            try:
                self.poll_messages(force=force)
            except Exception as ex:
                self.logger.info(f"{self.internal_id} run subscriber fails to poll messages: {ex}")

            self._is_stop = True
            self.stop()
            self.logger.info(f"{self.internal_id} subscriber finished.")
        except Exception as ex:
            self.logger.error(f"{self.internal_id} run subscriber failed with error: {ex}")
            self.logger.error(traceback.format_exc())
            self._is_stop = True
            self.stop()

    def get_results(self, nologs=True):
        old_nologs = self._nologs
        self._nologs = nologs
        rets = self.results
        if not self._nologs:
            self.logger.debug(f'{self.internal_id} percent {self.get_results_percentage()}, results: {rets}')

        percent = self.get_results_percentage()
        if percent >= self._wait_percent:
            self.stop()
            self.logger.info(f"{self.internal_id} Got results: {percent} (number of wrong keys: {self._num_wrong_keys})")
        self._nologs = old_nologs
        return rets

    def get_results_percentage(self):
        return self._results_percentage

    def subscribe(self, force=False):
        if not self._subscribed:
            atexit.register(self.stop)

            self._graceful_stop = GracefulEvent()
            thread = threading.Thread(target=self.run_subscriber, kwargs={'force': force}, name="RunSubscriber")
            thread.start()
            time.sleep(1)
            self._subscribed = True
            self._is_stop = False

    def stop(self):
        if self._graceful_stop:
            self._graceful_stop.set()
        self.disconnect()
        self._subscribed = False
        time_start = time.time()
        while not self._is_stop and time.time() - time_start < 60:
            time.sleep(1)
        self._is_stop = True

    def __del__(self):
        # self.stop()
        pass

    def wait_results(self, timeout=None, force_return_results=False):
        self.subscribe()

        get_results = False
        time_log = time.time()
        time_start = time.time()
        if timeout is None:
            self.logger.info(f"{self.internal_id} waiting for results")
        try:
            while not get_results and self._graceful_stop and not self._graceful_stop.is_set():
                self.get_results(nologs=True)
                percent = self.get_results_percentage()
                if time.time() - time_log > 600:  # 10 minutes
                    self.logger.info(f"{self.internal_id} waiting for results: {percent} (number of wrong keys: {self._num_wrong_keys})")
                    time_log = time.time()
                time.sleep(1)
                if self.is_all_results_available:
                    get_results = True
                    self.waiting_result_terminated = True
                    self.logger.info(f"{self.internal_id} Got result percentage {percent} is not smaller then wait_percent {self._wait_percent}, set waiting_result_terminated to True")
                if self._timeout is not None and self._timeout > 0 and time.time() - time_start > self._timeout:
                    # global timeout
                    self.logger.info(f"{self.internal_id} Waiting result timeout({self._timeout} seconds), set waiting_result_terminated to True")
                    get_results = True
                    self.waiting_result_terminated = True
                if timeout is not None and timeout > 0 and time.time() - time_start > timeout:
                    # local timeout
                    self.logger.info(f"{self.internal_id} timeout reached")
                    break

            percent = self.get_results_percentage()
            if timeout is None or time.time() - time_start > 600:
                self.logger.info(f"{self.internal_id} Got results: {percent} (number of wrong keys: {self._num_wrong_keys})")
        except Exception as ex:
            self.logger.error(f"Wait_results got some exception: {ex}")
            self.logger.error(traceback.format_exc())
            self._graceful_stop.set()

        if get_results or self._graceful_stop.is_set() or self.is_all_results_available or force_return_results:
            # stop the subscriber
            self._graceful_stop.set()
            # wait the subscriber to finish
            time.sleep(2)
            percent = self.get_results_percentage()
            self.logger.info(f"{self.internal_id} Got results: {percent} (number of wrong keys: {self._num_wrong_keys})")

            results = self.results
            return results
        return None

    def wait_result(self, timeout=None, force_return_results=False):
        self.wait_results(timeout=timeout, force_return_results=force_return_results)
        results = self.results
        return results

    def is_ok(self):
        try:
            self.subscribe(force=True)
            test_id = str(uuid.uuid4())
            self.publish(test_id, force=True)
            ret = self.wait_result(force_return_results=True)
            self.logger.info(f"{self.internal_id} AsyncResult: publish: {test_id}, received: {ret}")
            if test_id == ret:
                self.logger.info(f"{self.internal_id} AsyncResult is ok")
                return True
            else:
                self.logger.info(f"{self.internal_id} AsyncResult is not ok")
                return False
        except Exception as ex:
            self.logger.error(f"{self.internal_id} AsyncResult is not ok: {ex}")
            return False
