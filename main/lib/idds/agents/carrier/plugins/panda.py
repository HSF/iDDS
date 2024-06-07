#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2024


try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

import os
import traceback

from idds.common.constants import ProcessingStatus
from .base import BaseSubmitterPoller


class PandaSubmitterPoller(BaseSubmitterPoller):

    def __init__(self, *args, **kwargs):
        super(PandaSubmitterPoller, self).__init__()
        self.load_panda_urls()

    def load_panda_config(self):
        panda_config = ConfigParser.ConfigParser()
        if os.environ.get('IDDS_PANDA_CONFIG', None):
            configfile = os.environ['IDDS_PANDA_CONFIG']
            if panda_config.read(configfile) == [configfile]:
                return panda_config

        configfiles = ['%s/etc/panda/panda.cfg' % os.environ.get('IDDS_HOME', ''),
                       '/etc/panda/panda.cfg', '/opt/idds/etc/panda/panda.cfg',
                       '%s/etc/panda/panda.cfg' % os.environ.get('VIRTUAL_ENV', '')]
        for configfile in configfiles:
            if panda_config.read(configfile) == [configfile]:
                return panda_config
        return panda_config

    def load_panda_urls(self):
        panda_config = self.load_panda_config()
        # self.logger.debug("panda config: %s" % panda_config)
        self.panda_url = None
        self.panda_url_ssl = None
        self.panda_monitor = None
        self.panda_auth = None
        self.panda_auth_vo = None
        self.panda_config_root = None
        self.pandacache_url = None
        self.panda_verify_host = None

        if panda_config.has_section('panda'):
            if 'PANDA_MONITOR_URL' not in os.environ and panda_config.has_option('panda', 'panda_monitor_url'):
                self.panda_monitor = panda_config.get('panda', 'panda_monitor_url')
                os.environ['PANDA_MONITOR_URL'] = self.panda_monitor
                # self.logger.debug("Panda monitor url: %s" % str(self.panda_monitor))
            if 'PANDA_URL' not in os.environ and panda_config.has_option('panda', 'panda_url'):
                self.panda_url = panda_config.get('panda', 'panda_url')
                os.environ['PANDA_URL'] = self.panda_url
                # self.logger.debug("Panda url: %s" % str(self.panda_url))
            if 'PANDACACHE_URL' not in os.environ and panda_config.has_option('panda', 'pandacache_url'):
                self.pandacache_url = panda_config.get('panda', 'pandacache_url')
                os.environ['PANDACACHE_URL'] = self.pandacache_url
                # self.logger.debug("Pandacache url: %s" % str(self.pandacache_url))
            if 'PANDA_VERIFY_HOST' not in os.environ and panda_config.has_option('panda', 'panda_verify_host'):
                self.panda_verify_host = panda_config.get('panda', 'panda_verify_host')
                os.environ['PANDA_VERIFY_HOST'] = self.panda_verify_host
                # self.logger.debug("Panda verify host: %s" % str(self.panda_verify_host))
            if 'PANDA_URL_SSL' not in os.environ and panda_config.has_option('panda', 'panda_url_ssl'):
                self.panda_url_ssl = panda_config.get('panda', 'panda_url_ssl')
                os.environ['PANDA_URL_SSL'] = self.panda_url_ssl
                # self.logger.debug("Panda url ssl: %s" % str(self.panda_url_ssl))
            if 'PANDA_AUTH' not in os.environ and panda_config.has_option('panda', 'panda_auth'):
                self.panda_auth = panda_config.get('panda', 'panda_auth')
                os.environ['PANDA_AUTH'] = self.panda_auth
            if 'PANDA_AUTH_VO' not in os.environ and panda_config.has_option('panda', 'panda_auth_vo'):
                self.panda_auth_vo = panda_config.get('panda', 'panda_auth_vo')
                os.environ['PANDA_AUTH_VO'] = self.panda_auth_vo
            if 'PANDA_CONFIG_ROOT' not in os.environ and panda_config.has_option('panda', 'panda_config_root'):
                self.panda_config_root = panda_config.get('panda', 'panda_config_root')
                os.environ['PANDA_CONFIG_ROOT'] = self.panda_config_root

    def submit(self, work, logger=None, log_prefix=''):
        from pandaclient import Client

        task_params = self.get_task_params(work)
        try:
            return_code = Client.insertTaskParams(task_params, verbose=True)
            if return_code[0] == 0 and return_code[1][0] is True:
                try:
                    task_id = int(return_code[1][1])
                    return task_id, None
                except Exception as ex:
                    if logger:
                        logger.warn(log_prefix + "task id is not retruned: (%s) is not task id: %s" % (return_code[1][1], str(ex)))
                    if return_code[1][1] and 'jediTaskID=' in return_code[1][1]:
                        parts = return_code[1][1].split(" ")
                        for part in parts:
                            if 'jediTaskID=' in part:
                                task_id = int(part.split("=")[1])
                                return task_id, None
                    else:
                        raise Exception(return_code)
            else:
                if logger:
                    logger.warn(log_prefix + "submit_panda_task, return_code: %s" % str(return_code))
                raise Exception(return_code)
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex

    def get_processing_status(self, task_status):
        if task_status in ['registered', 'defined', 'assigning']:
            processing_status = ProcessingStatus.Submitting
        elif task_status in ['ready', 'scouting', 'scouted', 'prepared', 'topreprocess', 'preprocessing']:
            processing_status = ProcessingStatus.Submitting
        elif task_status in ['pending']:
            processing_status = ProcessingStatus.Submitted
        elif task_status in ['running', 'toretry', 'toincexec', 'throttled']:
            processing_status = ProcessingStatus.Running
        elif task_status in ['done']:
            processing_status = ProcessingStatus.Finished
        elif task_status in ['finished', 'paused']:
            # finished, finishing, waiting it to be done
            processing_status = ProcessingStatus.SubFinished
        elif task_status in ['failed', 'exhausted']:
            # aborting, tobroken
            processing_status = ProcessingStatus.Failed
        elif task_status in ['aborted']:
            # aborting, tobroken
            processing_status = ProcessingStatus.Cancelled
        elif task_status in ['broken']:
            processing_status = ProcessingStatus.Broken
        else:
            # finished, finishing, aborting, topreprocess, preprocessing, tobroken
            # toretry, toincexec, rerefine, paused, throttled, passed
            processing_status = ProcessingStatus.Submitted
        return processing_status

    def poll(self, workload_id, logger=None, log_prefix=''):
        from pandaclient import Client

        try:
            status, task_status = Client.getTaskStatus(workload_id)
            if status == 0:
                return self.get_processing_status(task_status)
            else:
                msg = "Failed to poll task %s: status: %s, task_status: %s" % (workload_id, status, task_status)
                raise Exception(msg)
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex

    def abort(self, workload_id, logger=None, log_prefix=''):
        from pandaclient import Client

        try:
            if logger:
                logger.info(log_prefix + f"aborting task {workload_id}")
            Client.killTask(workload_id, soft=True)
            status, task_status = Client.getTaskStatus(workload_id)
            if status == 0:
                return self.get_processing_status(task_status)
            else:
                msg = "Failed to abort task %s: status: %s, task_status: %s" % (workload_id, status, task_status)
                raise Exception(msg)
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex

    def resume(self, workload_id, logger=None, log_prefix=''):
        from pandaclient import Client

        try:
            if logger:
                logger.info(log_prefix + f"resuming task {workload_id}")
            status, out = Client.retryTask(workload_id, newParams={})
            return ProcessingStatus.Running
        except Exception as ex:
            if logger:
                logger.error(log_prefix + str(ex))
                logger.error(traceback.format_exc())
            raise ex
