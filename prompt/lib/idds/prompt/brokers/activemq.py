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
import os
import random
import socket
import threading
import time
import traceback
import stomp
import uuid

from cachetools import TTLCache

from idds.common.plugin.plugin_base import PluginBase
from idds.common.utils import setup_logging, json_dumps, json_loads


setup_logging(__name__)
# Allow enabling stomp debug via environment variable for diagnostics
if os.environ.get("STOMP_DEBUG") in ("1", "true", "True"):
    logging.getLogger("stomp").setLevel(logging.DEBUG)
else:
    logging.getLogger("stomp").setLevel(logging.CRITICAL)


class MessagingListener(stomp.ConnectionListener):
    """
    Messaging Listener
    """

    def __init__(
        self, broker, handler, handler_kwargs, conn, logger=None, subscriber=None, namespace=None
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
        self.namespace = namespace

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, frame):
        """
        Error handler
        """
        self.logger.error("[broker] [%s]: headers:%s, body: %s", self.__broker, frame.headers, frame.body)

    def on_disconnected(self):
        self.logger.warning("STOMP connection disconnected (server or transport).")
        if self.subscriber is not None:
            try:
                self.subscriber.fail()
                self.subscriber.monitor()
            except Exception:
                pass

    def on_connected(self, headers, body=None):
        """Log CONNECTED headers negotiated with the broker (if available).

        Different stomp.py versions may call this with a frame-like object or
        (headers, body). Be flexible when extracting headers.
        """
        try:
            hdrs = None
            if hasattr(headers, "headers"):
                # a frame-like object
                hdrs = headers.headers
            else:
                hdrs = headers
            self.logger.info("STOMP CONNECTED from broker %s: %s", self.__broker, hdrs)
        except Exception:
            self.logger.info("STOMP CONNECTED from broker %s (failed to extract headers)", self.__broker)

    def on_heartbeat_timeout(self):
        self.logger.warning("STOMP heartbeat timeout.")
        if self.subscriber is not None:
            self.subscriber.fail()

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
            # Some stomp.py versions do not accept a `subscription=` keyword
            # argument for ack/nack. Use positional args: (message-id, subscription).
            self.conn.ack(frame.headers["message-id"])
        except Exception as ex:
            self.logger.error(f"Failed to handle message: {ex}", exc_info=True)
            if self.subscriber is not None:
                self.subscriber.fail()

            # Attempt to nack using flexible signatures; if transport is gone,
            # trigger reconnect via subscriber.monitor().
            try:
                self.conn.nack(frame.headers["message-id"])
            except Exception:
                # If nack is unavailable or fails, log and move on.
                self.logger.exception("nack failed")

        # update last seen timestamp on the subscriber (idle detection)
        if self.subscriber is not None:
            try:
                self.subscriber.last_message_at = time.time()
                self.subscriber.is_processing_message = False
            except Exception:
                pass


class BaseActiveMQ(PluginBase):
    def __init__(
        self, name="BaseActiveMQ", namespace=None, logger=None, broker=None, lifetime=3600, **kwargs
    ):
        super(BaseActiveMQ, self).__init__(
            name=name, logger=logger, broker=broker, **kwargs
        )

        self.logger = logger
        self.setup_logger(self.logger)
        self.namespace = namespace

        self.has_connection_failures = False
        self.start_at = None
        self.lifetime = lifetime

        self.name = name
        self.broker = broker

        internal_id = str(uuid.uuid4())[:8]
        self.hostname = socket.getfqdn().split(".")[0]
        self.internal_id = f"{self.namespace}.{self.name}.{self.hostname}.{internal_id}"

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
            self.logger = logging.getLogger(logger_name)

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

                # Ask for IPv4 stream (TCP) addresses to avoid duplicate
                # entries for multiple socket types. Some platforms/DNS
                # configurations return the same address more than once;
                # we'll deduplicate below.
                addrinfos = socket.getaddrinfo(
                    b, 0, socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP
                )

                seen = set()
                for addrinfo in addrinfos:
                    b_addr = addrinfo[4][0]
                    try:
                        p = int(port)
                    except Exception:
                        p = port
                    pair = (b_addr, p)
                    if pair in seen:
                        continue
                    seen.add(pair)
                    broker_addresses.append(pair)
            except socket.gaierror as error:
                self.logger.error("Cannot resolve hostname %s: %s" % (b, str(error)))

        self.logger.info(
            "Resolved broker addresses for channel %s with brokers %s: %s"
            % (self.name, brokers, broker_addresses)
        )

        use_ssl = self.broker.get("use_ssl", False)
        ssl_ca_certs = self.broker.get("ssl_ca_certs", None)
        if use_ssl:
            import ssl
            ssl_version = (ssl.PROTOCOL_TLS_CLIENT if ssl_ca_certs else ssl.PROTOCOL_TLS)
        else:
            ssl_version = None

        conns = []
        for broker, port in broker_addresses:
            conn = stomp.Connection12(
                host_and_ports=[(broker, port)],
                keepalive=True,
                # Shorter heartbeats (ms) so client/broker detect dead peers faster
                heartbeats=(10000, 10000),
                # timeout=broker_timeout,
            )
            if use_ssl:
                conn.transport.set_ssl(
                    for_hosts=[(broker, port)],
                    ca_certs=ssl_ca_certs if use_ssl else None,
                    ssl_version=ssl_version,
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
            if self.conns:
                conn = random.sample(self.conns, 1)[0]
                if not conn.is_connected():
                    conn.connect(
                        self.broker["username"],
                        self.broker["password"],
                        wait=True,
                        headers={"client-id": self.internal_id}
                    )
                # conn.start()
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
                    self.broker["username"],
                    self.broker["password"],
                    wait=True,
                    headers={"client-id": self.internal_id}
                )
                # conn.start()
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
        - 'namespace': namespace
        - 'msg_type': message type
        - 'run_id': run identifier

        :param msg: Message dictionary to publish (should contain 'msg_type' and 'run_id')
        :param headers: Optional headers dictionary (can override defaults)
        """
        # msg_id = msg.get("msg_id", "unknown")
        namespace = getattr(self, "namespace", None)
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
            "msg_type": str(msg_type).lower(),
            "run_id": run_id,
            "client-id": self.internal_id,
        }

        # Override with custom headers if provided
        if headers:
            send_headers.update(headers)

        try:
            if namespace is not None:
                send_headers["namespace"] = namespace
            conn.send(
                body=json_dumps(msg),
                destination=self.broker["destination"],
                id=self.internal_id,
                ack="auto",
                headers=send_headers,
            )
            self.logger.debug(
                f"Message published successfully: msg_type={msg_type}, run_id={run_id}, destination={self.broker['destination']}"
            )
        except Exception as ex:
            self.logger.error(
                f"Failed to publish message: msg_type={msg_type}, run_id={run_id}, destination={self.broker['destination']}, error={ex}",
                exc_info=True,
            )


class Subscriber(BaseActiveMQ):
    def __init__(
        self,
        name="Subscriber",
        namespace=None,
        logger=None,
        broker=None,
        lifetime=1800,
        handler=None,
        handler_kwargs=None,
        selector=None,
        **kwargs,
    ):
        super(Subscriber, self).__init__(
            name=name, namespace=namespace, logger=logger, broker=broker, lifetime=lifetime, **kwargs
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
                namespace=self.namespace,
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
        conn.connect(
            self.broker["username"],
            self.broker["password"],
            wait=True,
            headers={"client-id": self.internal_id},
        )
        # conn.start()

        # conn.subscribe(destination=self.broker['destination'], id=f'{self.internal_id}',
        #                ack='client-individual', headers={'activemq.prefetchSize': '1'})
        # Build a broker-side selector so filtering happens before delivery.
        if self.namespace is not None:
            if self.selector:
                # Quote namespace to handle string values in selectors
                selector = f"namespace='{self.namespace}' AND ({self.selector})"
            else:
                selector = f"namespace='{self.namespace}'"
        else:
            if self.selector:
                selector = f"{self.selector}"
            else:
                selector = None

        # update last_message_at when subscription is established
        try:
            self.last_message_at = time.time()
        except Exception:
            pass

        headers = {
            "subscription-name": self.internal_id,
            "durable": False,
            "activemq.prefetchSize": "1",
        }

        if selector:
            headers["selector"] = selector

        conn.subscribe(
            destination=self.broker["destination"],
            id=f"{self.internal_id}",
            ack="client-individual",
            headers=headers,
        )
        self.logger.info(
            f"Subscribed to {self.broker['destination']} with selector: {selector} on broker {broker_info}, ack mode: client-individual, headers: {headers}"
        )

    def subscribe(self):
        if not self.conns:
            self.conns = self.connect_to_messaging_brokers()

        for conn in self.conns:
            if not conn.is_connected():
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
