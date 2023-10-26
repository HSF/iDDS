#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2023


import logging
import random
import socket
import threading
import time
import traceback
import stomp

from idds.common.plugin.plugin_base import PluginBase
from idds.common.utils import setup_logging, get_logger, json_dumps, json_loads


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
            self.logger = get_logger(self.__class__.__name__)

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
    def __init__(self, name="MessagingSender", logger=None, **kwargs):
        threading.Thread.__init__(self, name=name)
        super(MessagingSender, self).__init__(name=name, logger=logger, **kwargs)

        if logger:
            self.logger = logger
        self.graceful_stop = threading.Event()
        self.graceful_suspend = threading.Event()

        self.request_queue = None
        self.output_queue = None
        self.response_queue = None

        if not hasattr(self, 'channels'):
            raise Exception('"channels" is required but not defined.')
        self.channels = json_loads(self.channels)

        self.broker_timeout = 3600

        if not hasattr(self, 'timetolive'):
            self.timetolive = 12 * 3600 * 1000     # milliseconds
        else:
            self.timetolive = int(self.timetolive)

        self.conns = []

    def setup_logger(self, logger):
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(self.__class__.__name__)

    def set_logger(self, logger):
        self.logger = logger

    def get_logger(self):
        return self.logger

    def stop(self):
        self.graceful_stop.set()

    def suspend(self):
        self.graceful_suspend.set()

    def resume(self):
        self.graceful_suspend.clear()

    def is_processing(self):
        return (not self.graceful_stop.is_set()) and (not self.graceful_suspend.is_set())

    def set_request_queue(self, request_queue):
        self.request_queue = request_queue

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def set_response_queue(self, response_queue):
        self.response_queue = response_queue

    def connect_to_messaging_brokers(self, sender=True):
        channel_conns = {}
        for name in self.channels:
            channel = self.channels[name]
            if channel and 'brokers' in channel:
                brokers = channel['brokers']
                if type(brokers) in [list, tuple]:
                    pass
                else:
                    brokers = brokers.split(",")
                # destination = channel['destination']
                # username = channel['username']
                # password = channel['password']
                broker_timeout = channel['broker_timeout']

                broker_addresses = []
                for b in brokers:
                    try:
                        b, port = b.split(":")

                        addrinfos = socket.getaddrinfo(b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
                        for addrinfo in addrinfos:
                            b_addr = addrinfo[4][0]
                            broker_addresses.append((b_addr, port))
                    except socket.gaierror as error:
                        self.logger.error('Cannot resolve hostname %s: %s' % (b, str(error)))

                self.logger.info("Resolved broker addresses for channel %s: %s" % (name, broker_addresses))

                timeout = broker_timeout

                conns = []
                for broker, port in broker_addresses:
                    conn = stomp.Connection12(host_and_ports=[(broker, port)],
                                              keepalive=True,
                                              heartbeats=(30000, 30000),     # half minute = num / 1000
                                              timeout=timeout)
                    conns.append(conn)
                channel_conns[name] = conns
            else:
                channel_conns[name] = None
        return channel_conns

    def disconnect(self, conns):
        for name in conns:
            if conns[name]:
                for conn in conns[name]:
                    try:
                        if conn.is_connected():
                            conn.disconnect()
                    except Exception:
                        pass

    def get_connection(self, destination):
        try:
            if destination not in self.conns:
                destination = 'default'
            if self.conns[destination]:
                conn = random.sample(self.conns[destination], 1)[0]
                queue_dest = self.channels[destination]['destination']
                username = self.channels[destination]['username']
                password = self.channels[destination]['password']
                if not conn.is_connected():
                    # conn.start()
                    conn.connect(username, password, wait=True)
                return conn, queue_dest, destination
            elif self.conns[destination] is None:
                return None, None, destination
            else:
                # return None, None, destination
                pass
        except Exception as error:
            self.logger.error("Failed to connect to message broker(will re-resolve brokers): %s" % str(error))

        self.disconnect(self.conns)

        try:
            self.conns = self.connect_to_messaging_brokers(sender=True)
            if destination not in self.conns:
                destination = 'default'
            conn = random.sample(self.conns[destination], 1)[0]
            queue_dest = self.channels[destination]['destination']
            if not conn.is_connected():
                conn.connect(self.username, self.password, wait=True)
                return conn, queue_dest, destination
        except Exception as error:
            self.logger.error("Failed to connect to message broker(will re-resolve brokers): %s" % str(error))

    def send_message(self, msg):
        destination = msg['destination'] if 'destination' in msg else 'default'
        conn, queue_dest, destination = self.get_connection(destination)

        if conn:
            self.logger.info("Sending message to message broker(%s): %s" % (destination, msg['msg_id']))
            self.logger.debug("Sending message to message broker(%s): %s" % (destination, json_dumps(msg['msg_content'])))
            conn.send(body=json_dumps(msg['msg_content']),
                      destination=queue_dest,
                      id='atlas-idds-messaging',
                      ack='auto',
                      headers={'persistent': 'true',
                               'ttl': self.timetolive,
                               'vo': 'atlas',
                               'msg_type': str(msg['msg_type']).lower()})
        else:
            self.logger.info("No brokers defined, discard(%s): %s" % (destination, msg['msg_id']))

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
    def __init__(self, name="MessagingReceiver", logger=None, **kwargs):
        super(MessagingReceiver, self).__init__(name=name, logger=logger, **kwargs)
        self.listener = None
        self.receiver_conns = []

    def get_listener(self, broker):
        if self.listener is None:
            self.listener = MessagingListener(broker, self.output_queue, logger=self.logger)
        return self.listener

    def subscribe(self):
        self.receiver_conns = self.connect_to_messaging_brokers()

        for name in self.receiver_conns:
            for conn in self.receiver_conns[name]:
                self.logger.info('connecting to %s' % conn.transport._Transport__host_and_ports[0][0])
                conn.set_listener('message-receiver', self.get_listener(conn.transport._Transport__host_and_ports[0]))
                conn.connect(self.channels[name]['username'], self.channels[name]['password'], wait=True)
                conn.subscribe(destination=self.channels[name]['destination'], id='atlas-idds-messaging', ack='auto')

    def execute_subscribe(self):
        try:
            self.subscribe()
        except Exception as error:
            self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))

        sleep_count = 0
        while not self.graceful_stop.is_set():
            if self.graceful_suspend.is_set():
                try:
                    self.disconnect(self.receiver_conns)
                except Exception as error:
                    self.logger.error("Messaging receiver throws an exception: %s, %s" % (error, traceback.format_exc()))
                time.sleep(1)
                sleep_count += 1
                if sleep_count > 300:
                    self.logger.info("graceful_suspend is set. sleeping")
                    sleep_count = 0
            else:
                sleep_count = 0
                has_failed_connection = False
                try:
                    for name in self.receiver_conns:
                        for conn in self.receiver_conns[name]:
                            if not conn.is_connected():
                                conn.set_listener('message-receiver', self.get_listener(conn.transport._Transport__host_and_ports[0]))
                                # conn.start()
                                conn.connect(self.channels[name]['username'], self.channels[name]['password'], wait=True)
                                conn.subscribe(destination=self.channels[name]['destination'], id='atlas-idds-messaging', ack='auto')
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
    def __init__(self, name="MessagingMessager", logger=None, **kwargs):
        super(MessagingMessager, self).__init__(name=name, logger=logger, **kwargs)

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
                for name in self.receiver_conns:
                    for conn in self.receiver_conns[name]:
                        if not conn.is_connected():
                            conn.set_listener('message-receiver', self.get_listener(conn.transport._Transport__host_and_ports[0]))
                            # conn.start()
                            conn.connect(self.channels[name]['username'], self.channels[name]['password'], wait=True)
                            conn.subscribe(destination=self.channels[name]['destination'], id='atlas-idds-messaging', ack='auto')
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
