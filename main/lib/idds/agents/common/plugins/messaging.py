#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2022


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
    def __init__(self, broker, output_queue):
        '''
        __init__
        '''
        self.name = "MessagingListener"
        self.__broker = broker
        self.__output_queue = output_queue
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, frame):
        '''
        Error handler
        '''
        self.logger.error('[broker] [%s]: %s', self.__broker, frame.body)

    def on_message(self, frame):
        self.logger.debug('[broker] [%s]: %s', self.__broker, frame.body)
        self.__output_queue.put(frame.body)
        pass


class MessagingSender(PluginBase, threading.Thread):
    def __init__(self, name="MessagingSender", **kwargs):
        threading.Thread.__init__(self, name=name)
        super(MessagingSender, self).__init__(name=name, **kwargs)

        self.setup_logger()
        self.graceful_stop = threading.Event()
        self.request_queue = None
        self.output_queue = None
        self.response_queue = None

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
            self.broker_timeout = 60
        else:
            self.broker_timeout = int(self.broker_timeout)

        self.conns = []

    def stop(self):
        self.graceful_stop.set()

    def set_request_queue(self, request_queue):
        self.request_queue = request_queue

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def set_response_queue(self, response_queue):
        self.response_queue = response_queue

    def connect_to_messaging_brokers(self, sender=True):
        broker_addresses = []
        for b in self.brokers:
            try:
                if ":" in b:
                    b, port = b.split(":")
                else:
                    port = self.port

                addrinfos = socket.getaddrinfo(b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
                for addrinfo in addrinfos:
                    b_addr = addrinfo[4][0]
                    broker_addresses.append((b_addr, port))
            except socket.gaierror as error:
                self.logger.error('Cannot resolve hostname %s: %s' % (b, str(error)))

        self.logger.info("Resolved broker addresses: %s" % broker_addresses)

        timeout = self.broker_timeout

        conns = []
        for broker, port in broker_addresses:
            conn = stomp.Connection12(host_and_ports=[(broker, port)],
                                      vhost=self.vhost,
                                      keepalive=True,
                                      heartbeats=(60000, 60000),     # one minute
                                      timeout=timeout)
            conns.append(conn)
        return conns

    def disconnect(self, conns):
        for conn in conns:
            try:
                conn.disconnect()
            except Exception:
                pass

    def get_connection(self):
        try:
            conn = random.sample(self.conns, 1)[0]
            if not conn.is_connected():
                # conn.start()
                conn.connect(self.username, self.password, wait=True)
                return conn
        except Exception as error:
            self.logger.error("Failed to connect to message broker(will re-resolve brokers): %s" % str(error))

        self.disconnect(self.conns)

        self.conns = self.connect_to_messaging_brokers(sender=True)
        conn = random.sample(self.conns, 1)[0]
        if not conn.is_connected():
            conn.connect(self.username, self.password, wait=True)
            return conn

    def send_message(self, msg):
        conn = self.get_connection()

        self.logger.info("Sending message to message broker: %s" % msg['msg_id'])
        self.logger.debug("Sending message to message broker: %s" % json.dumps(msg['msg_content']))
        conn.send(body=json.dumps(msg['msg_content']),
                  destination=self.destination,
                  id='atlas-idds-messaging',
                  ack='auto',
                  headers={'persistent': 'true',
                           'vo': 'atlas',
                           'msg_type': str(msg['msg_type']).lower()})

    def execute_send(self):
        try:
            self.conns = self.connect_to_messaging_brokers(sender=True)
        except Exception as error:
            self.logger.error("Messaging sender throws an exception: %s, %s" % (error, traceback.format_exc()))

        while not self.graceful_stop.is_set():
            try:
                if not self.request_queue.empty():
                    msg = self.request_queue.get(False)
                    if msg:
                        self.send_message(msg)
                        self.response_queue.put(msg)
                else:
                    time.sleep(0.1)
            except Exception as error:
                self.logger.error("Messaging sender throws an exception: %s, %s" % (error, traceback.format_exc()))

    def run(self):
        try:
            self.execute_send()
        except Exception as error:
            self.logger.error("Messaging sender throws an exception: %s, %s" % (error, traceback.format_exc()))

    def __call__(self):
        self.run()


class MessagingReceiver(MessagingSender):
    def __init__(self, name="MessagingReceiver", **kwargs):
        super(MessagingReceiver, self).__init__(name=name, **kwargs)
        self.listener = None
        self.receiver_conns = []

    def get_listener(self, broker):
        if self.listener is None:
            self.listener = MessagingListener(broker, self.output_queue)
        return self.listener

    def subscribe(self):
        self.receiver_conns = self.connect_to_messaging_brokers()

        for conn in self.receiver_conns:
            self.logger.info('connecting to %s' % conn.transport._Transport__host_and_ports[0][0])
            conn.set_listener('message-receiver', self.get_listener(conn.transport._Transport__host_and_ports[0]))
            conn.connect(self.username, self.password, wait=True)
            conn.subscribe(destination=self.destination, id='atlas-idds-messaging', ack='auto')

    def execute_subscribe(self):
        try:
            self.subscribe()
        except Exception as error:
            self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

        while not self.graceful_stop.is_set():
            has_failed_connection = False
            try:
                for conn in self.receiver_conns:
                    if not conn.is_connected():
                        conn.set_listener('message-receiver', self.get_listener(conn.transport._Transport__host_and_ports[0]))
                        # conn.start()
                        conn.connect(self.username, self.password, wait=True)
                        conn.subscribe(destination=self.destination, id='atlas-idds-messaging', ack='auto')
                time.sleep(0.1)
            except Exception as error:
                self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))
                has_failed_connection = True

            if has_failed_connection or len(self.receiver_conns) == 0:
                try:
                    # re-subscribe
                    self.disconnect(self.receiver_conns)
                    self.subscribe()
                except Exception as error:
                    self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

        self.logger.info('receiver graceful stop requested')

        self.disconnect(self.receiver_conns)

    def run(self):
        try:
            self.execute_subscribe()
        except Exception as error:
            self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

    def __call__(self):
        self.run()


class MessagingMessager(MessagingReceiver):
    def __init__(self, name="MessagingMessager", **kwargs):
        super(MessagingMessager, self).__init__(name=name, **kwargs)

    def execute_send_subscribe(self):
        try:
            self.conns = self.connect_to_messaging_brokers(sender=True)
            self.subscribe()
        except Exception as error:
            self.logger.error("Messaging sender_subscriber throws an exception: %s, %s" % (error, traceback.format_exc()))

        while not self.graceful_stop.is_set():
            # send
            while True:
                try:
                    if not self.request_queue.empty():
                        msg = self.request_queue.get(False)
                        if msg:
                            self.send_message(msg)
                            if self.response_queue:
                                self.response_queue.put(msg)
                    else:
                        break
                except Exception as error:
                    self.logger.error("Messaging sender throws an exception: %s, %s" % (error, traceback.format_exc()))

            # subscribe
            has_failed_connection = False
            try:
                for conn in self.receiver_conns:
                    if not conn.is_connected():
                        conn.set_listener('message-receiver', self.get_listener(conn.transport._Transport__host_and_ports[0]))
                        # conn.start()
                        conn.connect(self.username, self.password, wait=True)
                        conn.subscribe(destination=self.destination, id='atlas-idds-messaging', ack='auto')
            except Exception as error:
                self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))
                has_failed_connection = True

            if has_failed_connection or len(self.receiver_conns) == 0:
                try:
                    # re-subscribe
                    self.disconnect(self.receiver_conns)
                    self.subscribe()
                except Exception as error:
                    self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

            time.sleep(0.1)

        self.logger.info('sender_receiver graceful stop requested')
        self.disconnect(self.conns)
        self.disconnect(self.receiver_conns)

    def run(self):
        try:
            self.execute_send_subscribe()
        except Exception as error:
            self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

    def __call__(self):
        self.run()
