#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2022

import json
import logging
import socket
import stomp
import threading
import time
import traceback
try:
    # python 3
    from queue import Queue
except ImportError:
    # Python 2
    from Queue import Queue

from idds.common.cache import get_cache, update_cache
from idds.common.constants import (Sections)
from idds.common.utils import setup_logging
from idds.agents.common.baseagent import BaseAgent

setup_logging(__name__)
logging.getLogger("stomp").setLevel(logging.CRITICAL)


class MessagingListener(stomp.ConnectionListener):
    '''
    Messaging Listener
    '''
    def __init__(self, broker, output_queue):
        '''
        __init__
        '''
        self.__broker = broker
        self.__output_queue = output_queue
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, headers, body):
        '''
        Error handler
        '''
        self.logger.error('[broker] [%s]: %s', self.__broker, body)

    def on_message(self, headers, body):
        # self.logger.info('[broker] [%s]: %s', self.__broker, body)
        self.__output_queue.put(body)
        pass


class MessagingReceiver(threading.Thread):
    def __init__(self, **kwargs):
        # threading.Thread.__init__(self)
        super(MessagingReceiver, self).__init__(**kwargs)

        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.logger = None
        self.setup_logger()
        self.graceful_stop = threading.Event()
        self.output_queue = None

        self.conns = []

    def get_class_name(self):
        return self.__class__.__name__

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.get_class_name())

    def stop(self):
        self.graceful_stop.set()

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def subscribe(self, listener=MessagingListener):
        self.conns = []

        broker_addresses = []
        for b in self.brokers:
            try:
                addrinfos = socket.getaddrinfo(b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
                for addrinfo in addrinfos:
                    b_addr = addrinfo[4][0]
                    broker_addresses.append(b_addr)
            except socket.gaierror as error:
                self.logger.error('Cannot resolve hostname %s: %s' % (b, str(error)))

        self.logger.info("Resolved broker addresses: %s" % broker_addresses)

        for broker in broker_addresses:
            conn = stomp.Connection12(host_and_ports=[(broker, self.port)],
                                      vhost=self.vhost,
                                      keepalive=True)
            conn.set_listener('message-receiver', listener(conn.transport._Transport__host_and_ports[0], self.output_queue))
            conn.connect(self.username, self.password, wait=True)
            conn.subscribe(destination=self.destination, id='atlas-idds-messaging', ack='auto')
            self.conns.append(conn)

        while not self.graceful_stop.is_set():
            try:
                for conn in self.conns:
                    if not conn.is_connected():
                        self.logger.info('connecting to %s' % conn.transport._Transport__host_and_ports[0][0])
                        conn.set_listener('message-receiver', listener(conn.transport._Transport__host_and_ports[0], self.output_queue))
                        # conn.start()
                        conn.connect(self.username, self.password, wait=True)
                        conn.subscribe(destination=self.destination, id='atlas-idds-messaging', ack='auto')
                time.sleep(1)
            except Exception as error:
                self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

        self.logger.info('receiver graceful stop requested')

        for conn in self.conns:
            try:
                conn.disconnect()
            except Exception:
                pass

    def run(self):
        try:
            self.subscribe()
        except Exception as error:
            self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

    def __call__(self):
        self.run()


class Receiver(BaseAgent):
    """
    Receiver works to receive messages and then update the processing or content status.
    """

    def __init__(self, num_threads=1, poll_time_period=10, retrieve_bulk_size=10, pending_time=None, **kwargs):
        super(Receiver, self).__init__(num_threads=num_threads, **kwargs)
        self.poll_time_period = int(poll_time_period)
        self.retrieve_bulk_size = int(retrieve_bulk_size)
        self.config_section = Sections.Receiver
        if pending_time:
            self.pending_time = float(pending_time)
        else:
            self.pending_time = None

        self.messageing_reciver = None
        self.messaging_queue = Queue()

        self.tasks = {}

        if not hasattr(self, 'messaging_brokers'):
            raise Exception('messaging brokers is required but not defined.')
        else:
            self.messaging_brokers = [b.strip() for b in self.messaging_brokers.split(',')]
        if not hasattr(self, 'messaging_port'):
            raise Exception('messaging port is required but not defined.')
        if not hasattr(self, 'messaging_vhost'):
            self.messaging_vhost = None
        if not hasattr(self, 'messaging_destination'):
            raise Exception('messaging destination is required but not defined.')
        if not hasattr(self, 'messaging_broker_timeout'):
            self.messaging_broker_timeout = 10
        else:
            self.messaging_broker_timeout = int(self.messaging_broker_timeout)

    def start_messaging_receiver(self):
        kwargs = {'broker': self.messaging_broker,
                  'port': self.messaging_port,
                  'vhost': self.messaging_vhost,
                  'destination': self.messaging_destinaton,
                  'broker_timeout': self.messaging_broker_timeout}
        self.messageing_reciver = MessagingReceiver(**kwargs)
        self.messageing_reciver.setup_output_queue(self.messaging_queue)

    def stop_receiver(self):
        self.messageing_reciver.stop()

    def handle_messages(self):
        try:
            while not self.messaging_queue.empty():
                msg = self.messaging_queue.get(False)
                if msg:
                    msg = json.loads(msg)
                    if 'msg_type' in msg:
                        if msg['msg_type'] == 'job_status' and 'taskid' in msg and 'status' in msg and msg['status'] in ['finished', 'failed']:
                            taskid = msg['taskid']
                            jobid = msg['jobid']
                            status = msg['status']
                            inputs = msg['inputs']
                            if taskid not in self.tasks:
                                self.tasks[taskid] = []
                            self.tasks[taskid].append({'taskid': taskid, 'jobid': jobid, 'status': status, 'inputs': inputs})
            for taskid in self.tasks:
                data = get_cache(taskid)
                if data:
                    self.tasks[taskid] = self.tasks[taskid] + data
                update_cache(taskid, self.tasks[taskid])
                del self.tasks[taskid]
        except Exception as error:
            self.logger.error("Failed to handle messages: %s, %s" % (error, traceback.format_exc()))

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.start_messaging_receiver()

            while not self.graceful_stop.is_set():
                self.handle_messages()
                time.sleep(self.poll_time_period)

        except KeyboardInterrupt:
            self.stop()
        finally:
            self.stop()

    def stop(self):
        super(Receiver, self).stop()
        self.stop_receiver()


if __name__ == '__main__':
    agent = Receiver()
    agent()
