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

from rucio.common.exception import DuplicateRule

from idds.common import exceptions
from idds.atlas.rucio.base_plugin import RucioPluginBase


class RuleSubmitter(RucioPluginBase):
    def __init__(self, lifetime=3600 * 24 * 7, **kwargs):
        super(RuleSubmitter, self).__init__(**kwargs)
        self.lifetime = int(lifetime)

    def __call__(self, processing, transform, input_collection):
        try:
            transform_metadata = transform['transform_metadata']
            if 'rule_id' in transform_metadata and transform_metadata['rule_id']:
                return transform_metadata['rule_id']

            did = {'scope': input_collection['scope'], 'name': input_collection['name']}

            try:
                rule_id = self.client.add_replication_rule(dids=[did],
                                                           copies=1,
                                                           rse_expression=transform_metadata['dest_rse'],
                                                           source_replica_expression=transform_metadata['src_rse'],
                                                           lifetime=self.lifetime,
                                                           locked=False,
                                                           grouping='DATASET',
                                                           ask_approval=False)
                if type(rule_id) in (list, tuple):
                    rule_id = rule_id[0]
                return rule_id
            except DuplicateRule as ex:
                self.logger.warn(ex)
                rules = self.client.list_did_rules(scope=input_collection['scope'], name=input_collection['name'])
                for rule in rules:
                    if rule['account'] == self.client.account and rule['rse_expression'] == transform_metadata['dest_rse']:
                        return rule['id']
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
