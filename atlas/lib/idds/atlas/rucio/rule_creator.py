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
Class of rule creator plubin
"""

import traceback

from rucio.common.exception import DuplicateRule

from idds.atlas.rucio.base_plugin import RucioPluginBase


class RuleCreator(RucioPluginBase):
    def __init__(self, lifetime=3600 * 24 * 7, **kwargs):
        super(RuleCreator, self).__init__(**kwargs)
        self.lifetime = int(lifetime)

    def __call__(self, dataset_scope, dataset_name, dids, dest_rse, src_rse=None, lifetime=None):
        try:
            # dids = [{'scope': <scope>, 'name': <name>}]
            if not lifetime:
                lifetime = self.lifetime

            self.client.add_dataset(scope=dataset_scope, name=dataset_name, lifetime=lifetime)
            self.client.attach_dids(scope=dataset_scope, name=dataset_name, dids=dids)
            self.client.set_status(scope=dataset_scope, name=dataset_name, open=False)

            ds_did = {'scope': dataset_scope, 'name': dataset_name}
            rule_id = self.client.add_replication_rule(dids=[ds_did],
                                                       copies=1,
                                                       rse_expression=dest_rse,
                                                       source_replica_expression=src_rse,
                                                       lifetime=lifetime,
                                                       locked=False,
                                                       grouping='DATASET',
                                                       ask_approval=False)
            if type(rule_id) in (list, tuple):
                rule_id = rule_id[0]
            return rule_id
        except DuplicateRule as ex:
            self.logger.warn(ex)
            rules = self.client.list_did_rules(scope=dataset_scope, name=dataset_name)
            for rule in rules:
                if rule['account'] == self.client.account and rule['rse_expression'] == dest_rse:
                    return rule['id']
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
        return None
