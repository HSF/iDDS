#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2023 - 2025

import logging
import os
import socket
import asyncio
import time
import traceback
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import NotFoundError as NATSNotFoundError

from idds.common.constants import (Sections)
from idds.common.exceptions import IDDSException
from idds.common.singleton import Singleton
from idds.common.utils import setup_logging, get_logger, json_loads, json_dumps
from idds.core import health as core_health
from idds.agents.common.baseagent import BaseAgent


logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("nats.aio.client").setLevel(logging.CRITICAL)
setup_logging(__name__)


class IDDSNATS(Singleton):
    def __init__(self, nats_server: dict, logger=None, debug_mode=False):
        if getattr(self, "_initialized", False):
            return
        self.nats_server = nats_server
        self.nc = None
        self.js = None
        self.logger = logger
        self._initialized = True
        self.debug_mode = debug_mode

    async def connect(self):
        nc = NATS()
        await nc.connect(
            servers=[self.nats_server["nats_url"]],
            token=self.nats_server["nats_token"]
        )
        js = nc.jetstream()
        if self.debug_mode and self.logger:
            self.logger.info(f"Connected to NATS: {self.nats_server}")
        return nc, js

    async def publish_event(self, event):
        nc = None
        try:
            nc, js = await self.connect()
            payload = json_dumps(event).encode("utf-8")
            ack = await js.publish(f"event.{event.event_type}", payload, timeout=5)
            if self.debug_mode and self.logger:
                self.logger.debug(f"Published event.{event.event_type} -> stream={ack.stream} seq={ack.seq}")
            return ack
        except Exception as e:
            if self.logger:
                self.logger.error(f"Publish failed for event {event.event_type}: event: {event}, error: {e}")
        finally:
            await self.close(nc)

    async def fetch_events(self, event_type_name, num_events=1, wait=5, callback=None):
        data_all = []
        nc = None
        try:
            nc, js = await self.connect()
            subject = f"event.{event_type_name}"
            durable = event_type_name
            subscriber = await js.pull_subscribe(subject, durable=durable)
            msgs = await subscriber.fetch(num_events, timeout=wait)
            for msg in msgs:
                data = msg.data.decode("utf-8")
                data = json_loads(data)
                if callback:
                    callback(data)
                await msg.ack()
                data_all.append(data)
            if self.logger:
                self.logger.debug(f"Fetched events from {event_type_name} using durable {durable}: {data_all}")
        except NATSTimeoutError:
            pass
        except NATSNotFoundError:
            if self.logger:
                self.logger.warning(f"Stream or consumer not found for event.{event_type_name}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to fetch event.{event_type_name}: {e}")
        finally:
            await self.close(nc)
        return data_all

    async def show(self):
        nc = None
        try:
            nc, js = await self.connect()
            streams = await js.streams_info()

            report_lines = []
            for stream_conf in streams:
                stream = stream_conf.config.name
                sinfo = await js.stream_info(stream)
                report_line = f"Stream {stream} info: {sinfo}"
                report_lines.append(report_line)

                consumers = await js.consumers_info(stream)
                for consumer_conf in consumers:
                    consumer = consumer_conf.name
                    report_line = f"  Consumer {consumer} config: {consumer_conf}"
                    report_lines.append(report_line)
                    cinfo = await js.consumer_info(stream, consumer)
                    report_line = f"  Consumer {consumer} info: {cinfo}"
                    report_lines.append(report_line)
            report = "\n".join(report_lines)
            if self.logger:
                self.logger.info(report)
            return report
        except Exception as ex:
            self.logger.error(f"Failed show stream information: {ex}")
            self.logger.error(traceback.format_exc())
        finally:
            await self.close(nc)

    async def close(self, nc=None):
        try:
            if nc:
                # await nc.drain()   # flush pending messages safely
                await nc.close()
                if self.debug_mode and self.logger:
                    self.logger.debug("NATS connection drained and closed")
        except Exception as ex:
            self.logger.error(f"Failed to close connection: {ex}")
        except NATSTimeoutError:
            pass


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
                 debug_mode=False,
                 use_process_pool=False,
                 **kwargs):
        super(NATSCoordinator, self).__init__(num_threads=num_threads, name='Coordinator', use_process_pool=use_process_pool, **kwargs)
        self.config_section = Sections.Coordinator

        self.debug_mode = bool(debug_mode)

        # -- config --
        self.coordination_interval_delay = int(coordination_interval_delay or 300)
        self.interval_delay = int(interval_delay or 30)
        self.max_boost_interval_delay = int(max_boost_interval_delay or 3)
        self.min_queued_events = int(min_queued_events or 10)
        self.max_queued_events = int(max_queued_events or 20)
        self.max_total_files_for_small_task = int(max_total_files_for_small_task or 1000)
        self.interval_delay_for_big_task = int(interval_delay_for_big_task or 300)
        self.show_queued_events_time_interval = int(show_queued_events_time_interval or 300)

        self.logger = get_logger(self.__class__.__name__)

        self.show_queued_events_time = None
        self.show_get_events_time = None
        self.show_get_events_time_interval = int(show_get_events_time_interval or 300)

        self.is_local_nats_ok = False
        self.nats_url_local = None
        self.nats_token_local = None
        self.init_local_nats()

        self.selected_nats_server = None
        self.idds_nats = None

    def get_hostname(self):
        hostname = socket.getfqdn()
        if "." in hostname and not hostname.endswith("."):
            return hostname
        fqdn = os.environ.get("POD_FQDN", None)
        if fqdn:
            if fqdn.startswith(hostname):
                return fqdn
            return f"{hostname}.{fqdn}"
        return hostname

    def init_local_nats(self):
        hostname = self.get_hostname()
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
        try:
            self.logger.debug(f"Set NATS server: {nats_server}")
            if not nats_server:
                if self.idds_nats:
                    asyncio.run(self.idds_nats.close())
                self.idds_nats = None
                self.selected_nats_server = None
                return None

            if self.selected_nats_server == nats_server and self.idds_nats:
                return self.idds_nats

            if self.idds_nats:
                asyncio.run(self.idds_nats.close())

            self.idds_nats = IDDSNATS(nats_server, logger=self.logger, debug_mode=self.debug_mode)
            self.selected_nats_server = nats_server
            return self.idds_nats
        except Exception as ex:
            self.logger.error(f"Failed to set selected nats: {ex}")
        return None

    def get_selected_nats_server(self):
        return self.idds_nats

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
        try:
            if self.idds_nats:
                asyncio.run(self.idds_nats.publish_event(event))
            else:
                self.logger.error(f"idds nats({self.idds_nats}) is not set, failed to send event {event}")
        except RuntimeError as ex:
            self.logger.error(f"Failed to send event: {ex}")
        except Exception as ex:
            self.logger.error(f"Failed to send event: {ex}")

    def send_bulk(self, events):
        for event in events:
            self.send(event)

    def get(self, event_type, num_events=1, wait=5, callback=None):
        try:
            if self.idds_nats:
                return asyncio.run(self.idds_nats.fetch_events(event_type_name=event_type.name, num_events=num_events, wait=wait, callback=callback))
            else:
                self.logger.error(f"idds nats({self.idds_nats}) is not set, failed to fetch events for {event_type.name}")
                return []
        except RuntimeError as ex:
            self.logger.error(f"Failed to get event: {ex}")
        except Exception as ex:
            self.logger.error(f"Failed to get event: {ex}")

    def send_report(self, event, status, start_time, end_time, source, result):
        try:
            pass
        except Exception as ex:
            self.logger.error(f"Failed to send event: {ex}")
            self.logger.error(traceback.format_exc())

    def clean_cache_info(self):
        pass

    def show_stream_info(self):
        try:
            if self.show_queued_events_time is None or self.show_queued_events_time + self.show_queued_events_time_interval > time.time():
                if self.idds_nats:
                    asyncio.run(self.idds_nats.show())
                self.show_queued_events_time = time.time()
        except RuntimeError as ex:
            self.logger.error(f"Failed to show stream info: {ex}")
        except Exception as ex:
            self.logger.error(f"Failed to show stream info: {ex}")

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
        if self.idds_nats:
            asyncio.run(self.idds_nats.close())
        super(NATSCoordinator, self).stop()


if __name__ == '__main__':
    agent = NATSCoordinator()
    agent()
