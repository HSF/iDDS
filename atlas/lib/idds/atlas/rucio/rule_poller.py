#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


"""
Class of collection lister plubin
"""

import traceback

from rucio.common.exception import RuleNotFound

from idds.common import exceptions
from idds.common.constants import ContentStatus
from idds.atlas.rucio.base_plugin import RucioPluginBase


class RulePoller(RucioPluginBase):
    def __init__(self, **kwargs):
        super(RulePoller, self).__init__(**kwargs)

    def get_state(self, lock_state):
        if lock_state == 'OK':
            return ContentStatus.Available
        return ContentStatus.New

    def __call__(self, rule_id):
        try:
            rule = self.client.get_replication_rule(rule_id=rule_id)
            # rule['state']

            replicases_status = {}
            if rule['locks_ok_cnt'] > 0:
                locks = self.client.list_replica_locks(rule_id=rule_id)
                for lock in locks:
                    scope_name = '%s:%s' % (lock['scope'], lock['name'])
                    replicases_status[scope_name] = self.get_state(lock['state'])
            return rule, replicases_status
        except RuleNotFound as ex:
            msg = "rule(%s) not found: %s" % (str(rule_id), str(ex))
            raise exceptions.ProcessNotFound(msg)
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
