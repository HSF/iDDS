#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2025

import os
import socket
import asyncio
import time
import threading
import traceback
from nats.errors import TimeoutError as NATSTimeoutError
from nats.aio.client import Client as NATS

from idds.common.constants import (Sections)
from idds.common.exceptions import IDDSException
from idds.common.utils import setup_logging, get_logger, json_loads, json_dumps
from idds.core import health as core_health
from idds.agents.common.baseagent import BaseAgent


setup_logging(__name__)


class NATSCoordinator(BaseAgent):
    """
    Coordinator works to schedule agents to process different events
    """

    def __init__(self, num_threads=1, coordination_interval_delay=300,
                 interval_delay=5, min_queued_events=10, max_queued_events=20,
                 max_total_files_for_small_task=1000,
                 interval_delay_for_big_task=60,
                 max_boost_interval_delay=3,
                 show_queued_events_time_interval=300,
                 show_get_events_time_interval=300,
                 **kwargs):
        super(NATSCoordinator, self).__init__(num_threads=num_threads, name='Coordinator', **kwargs)
        self.config_section = Sections.Coordinator

        # -- config --
        self.coordination_interval_delay = int(coordination_interval_delay or 300)
        self.interval_delay = int(interval_delay or 30)
        self.max_boost_interval_delay = int(max_boost_interval_delay or 3)
        self.min_queued_events = int(min_queued_events or 10)
        self.max_queued_events = int(max_queued_events or 20)
        self.max_total_files_for_small_task = int(max_total_files_for_small_task or 1000)
        self.interval_delay_for_big_task = int(interval_delay_for_big_task or 300)
        self.show_queued_events_time_interval = int(show_queued_events_time_interval or 300)

        self._lock = threading.RLock()
        self.logger = get_logger(self.__class__.__name__)

        self.show_queued_events_time = None
        self.show_get_events_time = None
        self.show_get_events_time_interval = int(show_get_events_time_interval or 300)

        self.is_local_nats_ok = False
        self.nats_url_local = None
        self.nats_token_local = None
        self.init_local_nats()

        self.selected_nats_server = None
        self.selected_nats = None

    def init_local_nats(self):
        hostname = socket.getfqdn()
        self.nats_url_local = f"nats://{hostname}:4222"
        self.nats_token_local = os.getenv("NATS_TOKEN", None) or "my_default_token"
        self.check_local_nats_status()

    def check_local_nats_status(self):
        async def check_nats():
            try:
                nc = NATS()
                await nc.connect(servers=[self.nats_url_local], token=self.nats_token_local)
                self.logger.debug("Connected to local NATS successfully")
                await nc.close()
                return True
            except Exception as e:
                self.logger.error(f"Failed to connect to local NATS: {e}")
                return False

        try:
            success = asyncio.run(check_nats())
        except RuntimeError:
            success = asyncio.get_event_loop().run_until_complete(check_nats())
        self.is_local_nats_ok = success
        return success

    def __del__(self):
        self.stop()

    def is_ready(self):
        return self.is_local_nats_ok

    def get_health_payload(self):
        payload = super(NATSCoordinator, self).get_health_payload()

        ok = self.check_local_nats_status()
        nats_server = {"nats_url": self.nats_url_local, "nats_token": self.nats_token_local} if ok else None

        if payload:
            payload['nats_server'] = nats_server
        else:
            payload = {'nats_server': nats_server}
        # payload = json_dumps(payload)
        return payload

    def set_selected_nats_server(self, nats_server):
        if not nats_server:
            if self.selected_nats is not None:
                asyncio.run(self.selected_nats.close())
            self.selected_nats_server = None
            self.logger.info(f"No selected NATS: {nats_server}")
            return False
        if not self.selected_nats_server or self.selected_nats_server != nats_server:
            # set new nats_server
            if self.selected_nats is not None:
                asyncio.run(self.selected_nats.close())
            self.selected_nats = NATS()
            asyncio.run(self.selected_nats.connect(
                servers=[nats_server["nats_url"]],
                token=nats_server["nats_token"]
            ))
            self.selected_nats_server = nats_server
            self.logger.info(f"Connected to selected NATS: {nats_server}")
            return True
        return False

    def get_selected_nats_server(self):
        if self.selected_nats:
            if not self.selected_nats.is_connected and self.selected_nats_server:
                asyncio.run(self.selected_nats.connect(
                    servers=[self.selected_nats_server["nats_url"]],
                    token=self.selected_nats_server["nats_token"]
                ))
            return self.selected_nats
        return None

    def select_coordinator(self):
        self.health_heartbeat(self.coordination_interval_delay)
        self.selected_coordinator = core_health.select_agent(
            name='NATSCoordinator',
            newer_than=self.coordination_interval_delay * 2
        )
        self.logger.debug("Selected coordinator: %s" % self.selected_coordinator)
        if self.selected_coordinator:
            payload = json_loads(self.selected_coordinator['payload'])
            selected_nats_server = payload['nats_server']
            ok = self.set_selected_nats_server(selected_nats_server)
            if ok:
                self.event_bus.set_coordinator(self)
            else:
                self.event_bus.set_coordinator(None)
        else:
            self.event_bus.set_coordinator(None)

    def send(self, event):
        async def publish_event(event):
            try:
                nc = self.get_selected_nats_server()
                if nc:
                    js = nc.jetstream()
                    await js.publish(f"event.{event.event_type}", json_dumps(event).encode("utf-8"))
                    # await nc.flush()
                    self.logger.debug(f"Published event.{event.event_type}: {json_dumps(event)}")
                else:
                    self.logger.error(f"Failed to published event.{event.event_type} because of NATS is not available(nats: {nc}): {json_dumps(event)}")
            except Exception as e:
                self.logger.error(f"Failed to publish event.{event.event_type}: {json_dumps(event)}: {e}")

        asyncio.run(publish_event(event))

    def send_bulk(self, events):
        for event in events:
            self.send(event)

    def get(self, event_type, num_events=1, wait=1, callback=None):
        async def fetch_events():
            try:
                nc = self.get_selected_nats_server()
                if nc:
                    js = nc.jetstream()
                    short_hostname = socket.gethostname().split(".")[0]
                    durable = f"event.{event_type}.{short_hostname}"
                    subscriber = await js.pull_subscribe(f"event.{event_type}", durable=durable)
                    msgs = await subscriber.fetch(num_events, timeout=wait)
                    data_all = []
                    for msg in msgs:
                        data = msg.data.decode("utf-8")
                        if callback:
                            callback(data)
                        await msg.ack()
                        data_all.append(data)
                    return data_all
                else:
                    if self.show_get_events_time is None or self.show_get_events_time + self.show_get_events_time_interval > time.time():
                        self.show_get_events_time = time.time()
                        self.logger.error(f"Failed to get event.{event_type} because of NATS is not available(nats: {nc})")
            except NATSTimeoutError:
                pass
            except Exception as e:
                self.logger.error(f"Failed to get event.{event_type}: {e}")
            return []
        return asyncio.run(fetch_events())

    def send_report(self, event, status, start_time, end_time, source, result):
        try:
            pass
        except Exception as ex:
            self.logger.error(f"Failed to send event: {ex}")
            self.logger.error(traceback.format_exc())

    def clean_cache_info(self):
        pass

    def show_stream_info(self):
        async def show():
            try:
                nc = self.get_selected_nats_server()
                if nc:
                    js = nc.jetstream()
                    streams = await js.stream_names()
                    report = ""
                    for stream in streams:
                        sinfo = await js.stream_info(stream)
                        report += f"Stream {stream} info: {sinfo}\n"

                        consumers = await js.consumer_names(stream)
                        for consumer in consumers:
                            cinfo = await js.consumer_info(stream, consumer)
                            report += f"    Consumer info: {cinfo}\n"
                    self.logger.info(report)
                else:
                    self.logger.error(f"Failed to show stream info because of NATS is not available(nats: {nc})")
            except Exception as ex:
                self.logger.error(f"Failed show stream information: {ex}")
                self.logger.error(traceback.format_exc())
        if self.show_queued_events_time is None or self.show_queued_events_time + self.show_queued_events_time_interval > time.time():
            asyncio.run(show())
            self.show_queued_events_time = time.time()

    def coordinate(self):
        self.select_coordinator()
        self.clean_cache_info()
        self.show_stream_info()

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")
            self.init_thread_info()
            # self.load_plugins()

            # coordinator will do the heartbeat by itself.
            # self.add_default_tasks()

            # self.add_health_message_task()

            while not self.graceful_stop.is_set():
                try:
                    self.coordinate()
                    time.sleep(self.coordination_interval_delay)
                except IDDSException as error:
                    self.logger.error("Main thread IDDSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
                # time.sleep(random.randint(5, self.random_delay))
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        super(NATSCoordinator, self).stop()
        if self.selected_nats and self.selected_nats.is_connected:
            asyncio.run(self.selected_nats.close())


if __name__ == '__main__':
    agent = NATSCoordinator()
    agent()
