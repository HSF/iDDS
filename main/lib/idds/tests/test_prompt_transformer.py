#!/usr/bin/env python
"""Small, deterministic unit tests for prompt message construction.

The original test file attempted to exercise live brokers and contained
syntax errors. These tests focus on the pure construction/contract of
messages so they remain reliable in CI without requiring an ActiveMQ
instance.
"""
import argparse
import logging
import os
import sys
import time

from idds.common.version import release_version
from idds.common.utils import setup_logging, json_loads

from idds.common.utils import get_prompt_broker_config
from idds.prompt.transformer import Transformer


setup_logging(__name__)


def get_parser():
    """
    Return the argparse parser.
    """
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)

    # common items
    oparser.add_argument(
        "--version", action="version", version="%(prog)s " + release_version
    )
    oparser.add_argument(
        "--verbose",
        "-v",
        default=False,
        action="store_true",
        help="Print more verbose output.",
    )
    oparser.add_argument(
        "--run_id", type=int, required=True, help="Run ID for the prompt run."
    )
    oparser.add_argument(
        "--workdir", type=str, required=False, default=None, help="Working directory for the prompt run."
    )
    oparser.add_argument(
        "--namespace", type=str, required=False, default=None, help="Namespace for the prompt run."
    )

    oparser.add_argument(
        "--idle_timeout",
        type=int,
        default=600,
        help="Idle timeout in seconds to stop the transformer if no messages are received.",
    )

    return oparser


def run():

    arguments = sys.argv[1:]

    oparser = get_parser()
    # argcomplete.autocomplete(oparser)

    args, unknown = oparser.parse_known_args(arguments)

    start_time = time.time()
    logging.info("Starting prompt with arguments: {0}".format(arguments))
    logging.info("Starting at: %-0.4f" % start_time)

    logging.info(f"called with args: {str(args)}")

    broker_info = get_prompt_broker_config()
    transformer_broker = broker_info.get("transformer_broker", None)
    transformer_broadcast_broker = broker_info.get(
        "transformer_broadcast_broker", None
    )
    result_broker = broker_info.get("result_broker", None)

    transformer = Transformer(run_id=args.run_id, workdir=args.workdir, namespace=args.namespace, idle_timeout=args.idle_timeout)
    transformer._transformer_broker = transformer_broker
    transformer._transformer_broadcast_broker = transformer_broadcast_broker
    transformer._result_broker = result_broker
    transformer._broker_initialized = True

    logging.info("Initialized transformer brokers")
    ret = transformer.run()
    logging.info("Transformer run returned: %s" % str(ret))
    return ret


if __name__ == "__main__":
    run()
