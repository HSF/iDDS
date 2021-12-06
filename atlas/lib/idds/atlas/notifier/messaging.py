#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import logging
import json
import random
import socket
import threading
import time
import traceback
import stomp

from idds.common.plugin.plugin_base import PluginBase
from idds.common.utils import setup_logging


setup_logging(__name__)
logging.getLogger("stomp").setLevel(logging.CRITICAL)


class MessagingListener(stomp.ConnectionListener):
    '''
    Messaging Listener
    '''
    def __init__(self, broker):
        '''
        __init__
        '''
        self.__broker = broker
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, headers, body):
        '''
        Error handler
        '''
        self.logger.error('[broker] [%s]: %s', self.__broker, body)

    def on_message(self, headers, body):
        # self.logger.info('[broker] [%s]: %s', self.__broker, body)
        pass


class MessagingSender(PluginBase, threading.Thread):
    def __init__(self, **kwargs):
        threading.Thread.__init__(self)
        super(MessagingSender, self).__init__(**kwargs)

        self.setup_logger()
        self.graceful_stop = threading.Event()
        self.request_queue = None
        self.output_queue = None

        if not hasattr(self, 'brokers'):
            raise Exception('brokers is required but not defined.')
        else:
            self.brokers = [b.strip() for b in self.brokers.split(',')]
        if not hasattr(self, 'port'):
            raise Exception('port is required but not defined.')
        if not hasattr(self, 'vhost'):
            self.vhost = None
        if not hasattr(self, 'destination'):
            raise Exception('destination is required but not defined.')
        if not hasattr(self, 'broker_timeout'):
            self.broker_timeout = 10
        else:
            self.broker_timeout = int(self.broker_timeout)

        self.conns = []

    def stop(self):
        self.graceful_stop.set()

    def set_request_queue(self, request_queue):
        self.request_queue = request_queue

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def connect_to_messaging_brokers(self):
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
                                      keepalive=True,
                                      timeout=self.broker_timeout)
            conn.set_listener('message-sender', MessagingListener(conn.transport._Transport__host_and_ports[0]))
            self.conns.append(conn)

    def send_message(self, msg):
        conn = random.sample(self.conns, 1)[0]
        if not conn.is_connected():
            # conn.start()
            conn.connect(self.username, self.password, wait=True)

        self.logger.info("Sending message to message broker: %s" % msg['msg_id'])
        self.logger.debug("Sending message to message broker: %s" % json.dumps(msg['msg_content']))
        conn.send(body=json.dumps(msg['msg_content']),
                  destination=self.destination,
                  id='atlas-idds-messaging',
                  ack='auto',
                  headers={'persistent': 'true',
                           'vo': 'atlas',
                           'msg_type': str(msg['msg_type']).lower()})

    def run(self):
        self.connect_to_messaging_brokers()

        while not self.graceful_stop.is_set():
            try:
                if not self.request_queue.empty():
                    msg = self.request_queue.get(False)
                    if msg:
                        self.send_message(msg)
                        self.output_queue.put(msg)
                else:
                    time.sleep(1)
            except Exception as error:
                self.logger.error("Messaging sender throws an exception: %s, %s" % (error, traceback.format_exc()))

    def __call__(self):
        self.run()


class MessagingReceiver(MessagingSender):
    def __init__(self, **kwargs):
        super(MessagingReceiver, self).__init__(**kwargs)

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
            conn.set_listener('message-receiver', listener(conn.transport._Transport__host_and_ports[0]))
            conn.connect(self.username, self.password, wait=True)
            conn.subscribe(destination=self.destination, id='atlas-idds-messaging', ack='auto')
            self.conns.append(conn)

        while not self.graceful_stop.is_set():
            try:
                for conn in self.conns:
                    if not conn.is_connected():
                        self.logger.info('connecting to %s' % conn.transport._Transport__host_and_ports[0][0])
                        conn.set_listener('message-receiver', listener(conn.transport._Transport__host_and_ports[0]))
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
