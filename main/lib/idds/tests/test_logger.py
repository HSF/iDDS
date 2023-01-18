#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022


import logging
import os
import sys

from idds.common.config import (config_has_section, config_has_option,
                                config_get)


def setup_logging(name, stream=None, loglevel=None):
    """
    Setup logging
    """
    if loglevel is None:
        if config_has_section('common') and config_has_option('common', 'loglevel'):
            loglevel = getattr(logging, config_get('common', 'loglevel').upper())
        else:
            loglevel = logging.INFO

    if stream is None:
        if config_has_section('common') and config_has_option('common', 'logdir'):
            logging.basicConfig(filename=os.path.join(config_get('common', 'logdir'), name),
                                level=loglevel,
                                format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')
        else:
            logging.basicConfig(stream=sys.stdout, level=loglevel,
                                format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')
    else:
        logging.basicConfig(stream=stream, level=loglevel,
                            format='%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')


def get_logger(name, filename=None, loglevel=None):
    """
    Setup logging
    """
    if loglevel is None:
        if config_has_section('common') and config_has_option('common', 'loglevel'):
            loglevel = getattr(logging, config_get('common', 'loglevel').upper())
        else:
            loglevel = logging.INFO

    if filename is None:
        filename = name + ".log"
    if not filename.startswith("/"):
        logdir = None
        if config_has_section('common') and config_has_option('common', 'logdir'):
            logdir = config_get('common', 'logdir')
        if not logdir:
            logdir = '/var/log/idds'
        filename = os.path.join(logdir, filename)

    formatter = logging.Formatter('%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s')

    handler = logging.FileHandler(filename)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def test_get_logger():
    logger = get_logger('test', filename='/tmp/wguan/test.log')
    logger.info("test")
    logger.debug("test1")
    print(logger.handlers)


if __name__ == '__main__':
    setup_logging('test1')
    test_get_logger()
