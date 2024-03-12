#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2024

import logging
import random
import socket
import stomp
import threading
import time
import traceback

from queue import Queue

from idds.common.constants import WorkflowType
from idds.common.utils import json_dumps, json_loads, setup_logging, get_unique_id_for_dict
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

    def get_result(self, name=None, args=None, key=None):
        logging.debug("get_result: key %s, name: %s, args: %s" % (key, name, args))
        logging.debug("get_result: results: %s, name_results: %s" % (self._results, self._name_results))

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
        return ret

    def get_all_results(self):
        return self._results


class AsyncResult(Base):

    def __init__(self, work_context, name=None, wait_num=1, wait_keys=[], group_kwargs=[], run_group_kwarg=None, map_results=False,
                 wait_percent=1, internal_id=None, timeout=None):
        """
        Init a workflow.
        """
        super(AsyncResult, self).__init__()
        if internal_id:
            self.internal_id = internal_id
        self._work_context = work_context

        self._name = name
        self._queue = Queue()

        self._connections = []
        self._graceful_stop = None
        self._subscribe_thread = None

        self._results = []
        self._bad_results = []
        self._results_percentage = 0
        self._map_results = map_results

        self._wait_num = wait_num
        if not self._wait_num:
            self._wait_num = 1
        self._wait_keys = set(wait_keys)
        self._group_kwargs = group_kwargs
        self._run_group_kwarg = run_group_kwarg

        self._wait_percent = wait_percent
        self._num_wrong_keys = 0

        self._timeout = timeout

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
        if self._group_kwargs:
            for kwargs in self._group_kwargs:
                k = get_unique_id_for_dict(kwargs)
                k = "%s:%s" % (self._name, k)
                self.logger.info("args (%s) to key: %s" % (str(kwargs), k))
                self._wait_keys.add(k)
            self._wait_num = len(self._wait_keys)
        return self._wait_keys

    @wait_keys.setter
    def wait_keys(self, value):
        self._wait_keys = set(value)

    @property
    def results(self):
        while not self._queue.empty():
            ret = self._queue.get()
            try:
                internal_id = ret['internal_id']
                if internal_id == self.internal_id:
                    self._results.append(ret)
                else:
                    self._bad_results.append(ret)
            except Exception as ex:
                self.logger.error("Received bad result: %s: %s" % (str(ret), str(ex)))
        if self._bad_results:
            self.logger.error("Received bad results: %s" % str(self._bad_results))

        self.logger.debug("_results: %s, bad_results: %s" % (str(self._results), str(self._bad_results)))
        self.logger.debug("wait_keys: %s, wait_num: %s" % (str(self.wait_keys), self._wait_num))
        rets_dict = {}
        for result in self._results:
            key = result['key']
            ret = result['ret']
            rets_dict[key] = ret

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

            if self._wait_num == 1:
                if rets:
                    return rets[0]
                else:
                    return None
            return rets

    @results.setter
    def results(self, value):
        raise Exception("Not allowed to set results.")
        if type(value) not in [list, tuple]:
            raise Exception("Results must be list or tuple, currently it is %s" % type(value))
        self._results = value

    def disconnect(self):
        for con in self._connections:
            try:
                if con.is_connected():
                    con.disconnect()
            except Exception:
                pass

    def connect_to_messaging_broker(self):
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

    def subscribe_to_messaging_brokers(self):
        workflow_context = self._work_context
        brokers = workflow_context.brokers
        conns = []

        broker_addresses = []
        for b in brokers.split(","):
            try:
                b, port = b.split(":")

                addrinfos = socket.getaddrinfo(b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
                for addrinfo in addrinfos:
                    b_addr = addrinfo[4][0]
                    broker_addresses.append((b_addr, port))
            except socket.gaierror as error:
                self.logger.error('Cannot resolve hostname %s: %s' % (b, str(error)))
                self._graceful_stop.set()

        self.logger.info("Resolved broker addresses: %s" % (broker_addresses))

        timeout = workflow_context.broker_timeout

        self.disconnect()

        listener = MessagingListener(brokers, self._queue, logger=self.logger)
        conns = []
        for broker, port in broker_addresses:
            conn = stomp.Connection12(host_and_ports=[(broker, port)],
                                      keepalive=True,
                                      heartbeats=(30000, 30000),     # half minute = num / 1000
                                      timeout=timeout)
            conn.set_listener("messag-subscriber", listener)
            conn.connect(workflow_context.broker_username, workflow_context.broker_password, wait=True)
            if workflow_context.type == WorkflowType.iWorkflow:
                subscribe_id = 'idds-workflow_%s' % self.internal_id
                # subscribe_selector = {'selector': "type = 'iworkflow' AND request_id = %s" % workflow_context.request_id}
                # subscribe_selector = {'selector': "type = 'iworkflow' AND internal_id = '%s'" % self.internal_id}
                subscribe_selector = {'selector': "internal_id = '%s'" % self.internal_id}
            elif workflow_context.type == WorkflowType.iWork:
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
            self.logger.info("subscribe to %s:%s with selector: %s" % (broker, port, subscribe_selector))
            conns.append(conn)
        self._connections = conns
        return conns

    def publish(self, ret, key=None):
        conn = self.connect_to_messaging_broker()
        workflow_context = self._work_context
        if key is None:
            if self._run_group_kwarg:
                key = get_unique_id_for_dict(self._run_group_kwarg)
                key = "%s:%s" % (self._name, key)
                self.logger.info("publish args (%s) to key: %s" % (str(self._run_group_kwarg), key))

        if workflow_context.type == WorkflowType.iWorkflow:
            headers = {'persistent': 'true',
                       'type': 'iworkflow',
                       'internal_id': str(self.internal_id),
                       'request_id': workflow_context.request_id}
            body = json_dumps({'ret': ret, 'key': key, 'internal_id': self.internal_id})
            conn.send(body=body,
                      destination=workflow_context.broker_destination,
                      id='idds-iworkflow_%s' % self.internal_id,
                      ack='auto',
                      headers=headers
                      )
            self.logger.info("publish header: %s, body: %s" % (str(headers), str(body)))
        elif workflow_context.type == WorkflowType.iWork:
            headers = {'persistent': 'true',
                       'type': 'iwork',
                       'internal_id': str(self.internal_id),
                       'request_id': workflow_context.request_id,
                       'transform_id': workflow_context.transform_id}
            body = json_dumps({'ret': ret, 'key': key, 'internal_id': self.internal_id})
            conn.send(body=body,
                      destination=workflow_context.broker_destination,
                      id='idds-iwork_%s' % self.internal_id,
                      ack='auto',
                      headers=headers
                      )
            self.logger.info("publish header: %s, body: %s" % (str(headers), str(body)))
        self.disconnect()

    def run_subscriber(self):
        try:
            self.logger.info("run subscriber")
            self.subscribe_to_messaging_brokers()
            while not self._graceful_stop.is_set():
                has_failed_conns = False
                for conn in self._connections:
                    if not conn.is_connected():
                        has_failed_conns = True
                if has_failed_conns:
                    self.subscribe_to_messaging_brokers()
                time.sleep(1)
        except Exception as ex:
            self.logger.error("run subscriber failed with error: %s" % str(ex))
            self.logger.error(traceback.format_exc())

    def get_results(self):
        rets = self.results
        self.logger.debug('results: %s' % str(rets))
        return rets

    def get_results_percentage(self):
        return self._results_percentage

    def subscribe(self):
        self._graceful_stop = threading.Event()
        thread = threading.Thread(target=self.run_subscriber, name="RunSubscriber")
        thread.start()
        time.sleep(1)
        self._subscribed = True

    def stop(self):
        if self._graceful_stop:
            self._graceful_stop.set()
        self.disconnect()

    def __del__(self):
        self.stop()

    def wait_results(self, timeout=None, force_return_results=False):
        if not self._subscribed:
            self.subscribe()

        get_results = False
        time_log = time.time()
        time_start = time.time()
        if timeout is None:
            self.logger.info("waiting for results")
        try:
            while not get_results and not self._graceful_stop.is_set():
                self.get_results()
                percent = self.get_results_percentage()
                if time.time() - time_log > 600:  # 10 minutes
                    self.logger.info("waiting for results: %s (number of wrong keys: %s)" % (percent, self._num_wrong_keys))
                    time_log = time.time()
                time.sleep(1)
                if percent >= self._wait_percent:
                    get_results = True
                if self._timeout is not None and self._timeout > 0 and time.time() - time_start > self._timeout:
                    # global timeout
                    self.logger.info("Waiting result timeout(%s seconds)" % self._timeout)
                    get_results = True
                if timeout is not None and timeout > 0 and time.time() - time_start > timeout:
                    # local timeout
                    break

            percent = self.get_results_percentage()
            if timeout is None or time.time() - time_start > 600:
                self.logger.info("Got results: %s (number of wrong keys: %s)" % (percent, self._num_wrong_keys))
        except Exception as ex:
            self.logger.error("Wait_results got some exception: %s" % str(ex))
            self.logger.error(traceback.format_exc())
            self._graceful_stop.set()

        if get_results or self._graceful_stop.is_set() or percent >= self._wait_percent or force_return_results:
            # stop the subscriber
            self._graceful_stop.set()
            self.logger.info("Got results: %s (number of wrong keys: %s)" % (percent, self._num_wrong_keys))

            results = self.results
            return results
        return None

    def wait_result(self, timeout=None, force_return_results=False):
        self.wait_results(timeout=timeout, force_return_results=force_return_results)
        results = self.results
        return results
