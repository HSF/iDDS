#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2025


import logging
import random
import socket
import threading
import time
import traceback
import stomp
import uuid

from cachetools import TTLCache

from idds.common.plugin.plugin_base import PluginBase
from idds.common.utils import setup_logging, get_logger, json_dumps, json_loads


setup_logging(__name__)
logging.getLogger("stomp").setLevel(logging.CRITICAL)


class MessagingListener(stomp.ConnectionListener):
    """
    Messaging Listener
    """

    def __init__(
        self, broker, handler, handler_kwargs, conn, logger=None, subscriber=None, instance=None
    ):
        """
        __init__
        """
        super(MessagingListener, self).__init__()
        # self.name = "MessagingListener"
        self.__broker = broker
        self.handler = handler
        self.handler_kwargs = handler_kwargs
        self.conn = conn
        # optional reference to the Subscriber that created this listener
        self.subscriber = subscriber
        self.instance = instance

        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(self.__class__.__name__)

    def on_error(self, frame):
        """
        Error handler
        """
        self.logger.error("[broker] [%s]: %s", self.__broker, frame.body)

    def on_message(self, frame):
        self.logger.debug(
            f"[broker] [{self.__broker}]: headers: {frame.headers}, body: {frame.body}"
        )
        headers = frame.headers
        try:
            if self.subscriber is not None:
                try:
                    self.subscriber.is_processing_message = True
                except Exception:
                    pass

            self.handler(headers, json_loads(frame.body), self.handler_kwargs)
            self.conn.ack(
                id=frame.headers["message-id"], subscription=headers["subscription"]
            )
        except Exception as ex:
            self.logger.error(f"Failed to handle message: {ex}", exc_info=True)
            self.conn.nack(
                id=frame.headers["message-id"], subscription=headers["subscription"]
            )

        # update last seen timestamp on the subscriber (idle detection)
        if self.subscriber is not None:
            try:
                self.subscriber.last_message_at = time.time()
                self.subscriber.is_processing_message = False
            except Exception:
                pass


class BaseActiveMQ(PluginBase):
    def __init__(
        self, name="BaseActiveMQ", instance=None, logger=None, broker=None, lifetime=3600, **kwargs
    ):
        super(BaseActiveMQ, self).__init__(
            name=name, logger=logger, broker=broker, **kwargs
        )

        self.logger = logger
        self.instance = instance

        self.has_connection_failures = False
        self.start_at = None
        self.lifetime = lifetime

        self.name = name
        self.broker = broker

        internal_id = str(uuid.uuid4())[:8]
        self.hostname = socket.getfqdn().split(".")[0]
        self.internal_id = f"{self.name}.{self.hostname}.{internal_id}"

        if not hasattr(self, "timetolive"):
            self.timetolive = 12 * 3600 * 1000  # milliseconds
        else:
            self.timetolive = int(self.timetolive)

        self.conns = []
        self.cache = TTLCache(maxsize=200, ttl=86400)  # cache expiration 86400 seconds
        self.graceful_stop = threading.Event()

    def setup_logger(self, logger):
        if logger:
            self.logger = logger
        else:
            logger_name = self.name if self.name else f"{self.__class__.__name__}"
            self.logger = get_logger(logger_name)

    def get_logger(self):
        return self.logger

    def stop(self):
        self.graceful_stop.set()
        self.disconnect(self.conns)

    def connect_to_messaging_brokers(self, sender=True):
        if self.conns:
            self.disconnect(self.conns)

        brokers = self.broker["brokers"]
        if type(brokers) in [list, tuple]:
            pass
        else:
            brokers = brokers.split(",")

        broker_timeout = self.broker.get("broker_timeout", 10)

        broker_addresses = []
        for b in brokers:
            try:
                b, port = b.split(":")

                addrinfos = socket.getaddrinfo(
                    b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP
                )
                for addrinfo in addrinfos:
                    b_addr = addrinfo[4][0]
                    try:
                        broker_addresses.append((b_addr, int(port)))
                    except Exception:
                        broker_addresses.append((b_addr, port))
            except socket.gaierror as error:
                self.logger.error("Cannot resolve hostname %s: %s" % (b, str(error)))

        self.logger.info(
            "Resolved broker addresses for channel %s: %s"
            % (self.name, broker_addresses)
        )

        conns = []
        for broker, port in broker_addresses:
            conn = stomp.Connection12(
                host_and_ports=[(broker, port)],
                keepalive=True,
                heartbeats=(30000, 30000),  # half minute = num / 1000
                timeout=broker_timeout,
            )
            conns.append(conn)
        self.conns = conns

        self.has_connection_failures = False

        return self.conns

    def disconnect(self, conns):
        for conn in conns:
            try:
                if conn.is_connected():
                    conn.disconnect()
            except Exception:
                pass

    def fail(self):
        self.has_connection_failures = True

    def get_connection(self):
        try:
            conn = random.sample(self.conns, 1)[0]
            if not conn.is_connected():
                conn.connect(
                    self.broker["username"], self.broker["password"], wait=True
                )
            return conn
        except Exception as error:
            self.logger.error(
                "Failed to connect to message broker(will re-resolve brokers): %s"
                % str(error)
            )

        self.disconnect(self.conns)

        try:
            self.conns = self.connect_to_messaging_brokers(sender=True)
            conn = random.sample(self.conns, 1)[0]
            if not conn.is_connected():
                conn.connect(
                    self.broker["username"], self.broker["password"], wait=True
                )
            return conn
        except Exception as error:
            self.logger.error(
                "Failed to connect to message broker(will re-resolve brokers): %s"
                % str(error)
            )

        self.fail()
        return None

    def monitor(self):
        try:
            if (
                not self.conns
                or not self.start_at
                or self.has_connection_failures
                or self.start_at + self.lifetime < time.time()
            ):
                self.conns = self.connect_to_messaging_brokers(sender=True)
                self.start_at = time.time()
        except Exception as error:
            self.logger.error(
                "Setup connections throws an exception: %s, %s"
                % (error, traceback.format_exc())
            )


class Publisher(BaseActiveMQ):
    def __init__(
        self,
        name="Publisher",
        instance=None,
        logger=None,
        broker=None,
        lifetime=3600,
        broadcast=False,
        **kwargs,
    ):
        super(Publisher, self).__init__(
            name=name, instance=instance, logger=logger, broker=broker, lifetime=lifetime, **kwargs
        )
        self.broadcast = broadcast

    def publish(self, msg, headers=None):
        """
        Publish a message to the broker.

        Based on prompt.md, all message headers should contain:
        - 'persistent': 'true'
        - 'ttl': time to live in milliseconds
        - 'vo': 'eic'
        - 'instance': instance name
        - 'msg_type': message type
        - 'run_id': run identifier

        :param msg: Message dictionary to publish (should contain 'msg_type' and 'run_id')
        :param headers: Optional headers dictionary (can override defaults)
        """
        # msg_id = msg.get("msg_id", "unknown")
        instance = getattr(self, "instance", "unknown")
        msg_type = msg.get("msg_type", "unknown")
        run_id = msg.get("run_id", "unknown")

        self.logger.debug(f"Publishing message: msg_type={msg_type}, run_id={run_id}")

        conn = self.get_connection()
        if not conn:
            self.logger.error(
                f"No connection available to send message: msg_type={msg_type}, run_id={run_id}"
            )
            return

        # Prepare headers according to prompt.md specifications
        send_headers = {
            "persistent": "true",
            "ttl": self.timetolive,
            "vo": "eic",
            "instance": instance,
            "msg_type": str(msg_type).lower(),
            "run_id": run_id,
        }

        # Override with custom headers if provided
        if headers:
            send_headers.update(headers)

        try:
            msg["instance"] = instance
            conn.send(
                body=json_dumps(msg),
                destination=self.broker["destination"],
                id=self.internal_id,
                ack="auto",
                headers=send_headers,
            )
            self.logger.debug(
                f"Message published successfully: msg_type={msg_type}, run_id={run_id}"
            )
        except Exception as ex:
            self.logger.error(
                f"Failed to publish message: msg_type={msg_type}, run_id={run_id}, error={ex}",
                exc_info=True,
            )


class Subscriber(BaseActiveMQ):
    def __init__(
        self,
        name="Subscriber",
        instance=None,
        logger=None,
        broker=None,
        lifetime=1800,
        handler=None,
        handler_kwargs=None,
        selector=None,
        **kwargs,
    ):
        super(Subscriber, self).__init__(
            name=name, instance=instance, logger=logger, broker=broker, lifetime=lifetime, **kwargs
        )
        self.listener = None
        self.handler = handler
        self.handler_kwargs = handler_kwargs if handler_kwargs else {}
        self.selector = selector
        # idle detection: timestamp of last message received
        self.last_message_at = time.time()

        # default idle threshold in seconds (can be overridden by passing 'idle_seconds' in kwargs)
        self.idle_seconds = int(kwargs.get("idle_seconds", 5))

        self.is_processing_message = False

    def get_listener(self, broker, conn):
        if self.listener is None:
            self.listener = MessagingListener(
                broker,
                instance=self.instance,
                handler=self.handler,
                handler_kwargs=self.handler_kwargs,
                conn=conn,
                logger=self.logger,
                subscriber=self,
            )
        return self.listener

    def subscribe_conn(self, conn):
        broker_info = conn.transport._Transport__host_and_ports[0][0]
        self.logger.info(f"connecting to: {broker_info}")
        conn.set_listener("message-receiver", self.get_listener(broker_info, conn=conn))
        conn.connect(self.broker["username"], self.broker["password"], wait=True)
        # conn.subscribe(destination=self.broker['destination'], id=f'{self.internal_id}',
        #                ack='client-individual', headers={'activemq.prefetchSize': '1'})
        # Build a broker-side selector so filtering happens before delivery.
        if self.selector:
            # Quote instance to handle string values in selectors
            selector = f"instance='{self.instance}' AND ({self.selector})"
        else:
            selector = f"instance='{self.instance}'"

        # update last_message_at when subscription is established
        try:
            self.last_message_at = time.time()
        except Exception:
            pass

        conn.subscribe(
            destination=self.broker["destination"],
            id=f"{self.internal_id}",
            ack="client-individual",
            headers={"selector": selector, "activemq.prefetchSize": "1"},
        )

    def subscribe(self):
        self.conns = self.connect_to_messaging_brokers()

        for conn in self.conns:
            self.subscribe_conn(conn)

    def is_idle(self, idle_seconds=None):
        """Return True if no message has been received for at least idle_seconds."""
        if self.is_processing_message:
            return False

        if idle_seconds is None:
            idle_seconds = self.idle_seconds
        last = getattr(self, "last_message_at", 0)
        return (time.time() - last) > float(idle_seconds)

    def wait_for_idle(self, timeout=None, poll_interval=0.5):
        """Block until the subscriber is idle or timeout (seconds) elapses.

        Returns True if idle was reached, False if timeout occurred.
        """
        start = time.time()
        while True:
            if self.is_idle():
                return True
            if timeout is not None and (time.time() - start) >= timeout:
                return False
            time.sleep(poll_interval)

    def monitor(self):
        try:
            if (
                not self.conns
                or not self.start_at
                or self.has_connection_failures
                or self.start_at + self.lifetime < time.time()
            ):
                self.subscribe()
                self.start_at = time.time()
        except Exception as error:
            self.logger.error(
                "Setup connections throws an exception: %s, %s"
                % (error, traceback.format_exc())
            )
