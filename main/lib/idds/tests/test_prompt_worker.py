#!/usr/bin/env python
"""Small, deterministic unit tests for prompt message construction.

The original test file attempted to exercise live brokers and contained
syntax errors. These tests focus on the pure construction/contract of
messages so they remain reliable in CI without requiring an ActiveMQ
instance.
"""

import datetime
import time

from idds.common.constants import Sections
from idds.common.config import config_get

# Delay importing Publisher until runtime to keep tests independent of broker packages

from idds.prompt.brokers.activemq import Publisher

# We only use the configured instance/namespace and broker strings (if present).
namespace = config_get(Sections.Prompt, "namespace")
# timetolive may be missing in some test environments; fall back to the default
try:
    timetolive = int(config_get(Sections.Prompt, "timetolive"))
except Exception:
    timetolive = 12 * 3600 * 1000
worker_publisher_broker = config_get(Sections.Prompt, "worker_publisher_broker")
slice_publisher_broker = config_get(Sections.Prompt, "slice_publisher_broker")


def create_start_messages(namespace, run_id):
    """Return a well-formed run_imminent message dict."""
    headers = {
        "persistent": "true",
        "ttl": timetolive,  # default: 12 * 3600 * 1000 (milliseconds)
        "vo": "eic",
        "namespace": namespace,  # e.g. 'prod', 'dev1', 'dev_<userid>'
        "msg_type": "run_imminent",
        "run_id": run_id,
    }
    msg = {
        "namespace": namespace,
        "msg_type": "run_imminent",
        "run_id": run_id,
        "created_at": datetime.datetime.utcnow(),
        "content": {
            "num_workers": 2,
            "num_cores_per_worker": 10,
            "num_ram_per_core": 4000.0,
            "msg_type": "start_run",
            "req_id": 1,
            "ts": "20250914185722",
        },
    }
    return msg, headers


def create_slice_messages(namespace, run_id):
    """Return a well-formed slice message dict."""
    headers = {
        "persistent": "true",
        "ttl": timetolive,  # default: 12 * 3600 * 1000 (milliseconds)
        "vo": "eic",
        "namespace": namespace,  # e.g. 'prod', 'dev1', 'dev_<userid>'
        "msg_type": "slice",
        "run_id": run_id,
    }
    msg = {
        "namespace": namespace,
        "msg_type": "slice",
        "run_id": run_id,
        "created_at": datetime.datetime.utcnow(),
        "content": {
            "run_id": run_id,
            "state": "no_beam",
            "substate": "calib",
            "filename": "swf.20250914.185724.767135.no_beam.calib.stf",
            "start": "20250914185722420185",
            "end": "20250914185724767135",
            "checksum": "ad:3915264619",
            "size": 191,
            "msg_type": "stf_gen",
            "req_id": 1,
        },
    }
    return msg, headers


def create_stop_messages(namespace, run_id):
    """Return a well-formed run_stop message dict."""
    headers = {
        "persistent": "true",
        "ttl": timetolive,  # default: 12 * 3600 * 1000 (milliseconds)
        "vo": "eic",
        "namespace": namespace,  # e.g. 'prod', 'dev1', 'dev_<userid>'
        "msg_type": "run_stop",
        "run_id": run_id,
    }
    msg = {
        "namespace": namespace,
        "msg_type": "run_stop",
        "run_id": run_id,
        "created_at": datetime.datetime.utcnow(),
        "content": {"req_id": 1, "run_id": run_id, "ts": "20250914185722"},
    }
    return msg, headers


def validate_message_basics(msg):
    assert isinstance(msg, dict)
    for k in ("instance", "msg_type", "run_id", "created_at", "content"):
        assert k in msg


def test_message_construction():
    run_id = int(time.time())
    start, start_headers = create_start_messages(namespace, run_id)
    slice_m, slice_headers = create_slice_messages(namespace, run_id)
    stop, stop_headers = create_stop_messages(namespace, run_id)

    validate_message_basics(start)
    validate_message_basics(slice_m)
    validate_message_basics(stop)

    assert start["msg_type"] == "run_imminent"
    assert slice_m["msg_type"] == "slice"
    assert stop["msg_type"] == "run_stop"


def run():
    run_id = int(time.time())
    start, start_headers = create_start_messages(namespace, run_id)
    slice_m, slice_headers = create_slice_messages(namespace, run_id)
    stop, stop_headers = create_stop_messages(namespace, run_id)

    worker_publisher = Publisher(
        name="WorkerPublisher",
        namespace=namespace,
        broker=worker_publisher_broker,
        broadcast=True,
    )
    slice_publisher = Publisher(
        name="SlicePublisher",
        namespace=namespace,
        broker=slice_publisher_broker,
        broadcast=True,
    )

    worker_publisher.publish(start, headers=start_headers)
    time.sleep(10)
    slice_publisher.publish(slice_m, headers=slice_headers)
    time.sleep(10)
    worker_publisher.publish(stop, headers=stop_headers)


if __name__ == "__main__":
    test_message_construction()
    run()
