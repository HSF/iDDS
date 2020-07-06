#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019-2020


"""
Class of collection lister plubin
"""

import datetime
import time
import traceback


from idds.common import exceptions
from idds.common.constants import ContentStatus, ProcessingStatus
from idds.atlas.processing.base_plugin import ProcessingPluginBase


class StageInPoller(ProcessingPluginBase):
    def __init__(self, **kwargs):
        super(StageInPoller, self).__init__(**kwargs)
        if hasattr(self, 'default_max_waiting_time'):
            self.default_max_waiting_time = int(self.default_max_waiting_time)
        else:
            self.default_max_waiting_time = 9999999999999999
        if hasattr(self, 'check_all_rules_for_new_rule'):
            if type(self.check_all_rules_for_new_rule) in [bool]:
                pass
            elif type(self.check_all_rules_for_new_rule) in [str]:
                if self.check_all_rules_for_new_rule.lower == 'true':
                    self.check_all_rules_for_new_rule = True
                else:
                    self.check_all_rules_for_new_rule = False
            else:
                self.check_all_rules_for_new_rule = False
        else:
            self.check_all_rules_for_new_rule = False
        if hasattr(self, 'new_rule_lifetime'):
            self.new_rule_lifetime = int(self.new_rule_lifetime)
        else:
            self.new_rule_lifetime = 3600 * 24 * 7

    def get_rule_lifetime(self, rule):
        current_time = datetime.datetime.utcnow()
        life_diff = current_time - rule['created_at']
        life_time = life_diff.total_seconds()
        # self.logger.info("rule created_at %s, rule life time %s" % (rule['created_at'], life_time))
        return life_time

    def get_max_waiting_time(self, transform):
        transform_metadata = transform['transform_metadata']
        if 'max_waiting_time' in transform_metadata and transform_metadata['max_waiting_time']:
            return transform_metadata['max_waiting_time']
        else:
            return self.default_max_waiting_time

    def should_create_new_rule(self, basic_rule, new_rules, transform):
        if new_rules:
            # self.logger.info("There are already new rules(%s), will not create new rule." % str(new_rules))
            return False
        if self.check_all_rules_for_new_rule:
            rules = [basic_rule] + new_rules
        else:
            rules = [basic_rule]

        # self.logger.debug("max_waiting_time: %s" % self.get_max_waiting_time(transform))
        for rule in rules:
            if rule and self.get_rule_lifetime(rule) >= self.get_max_waiting_time(transform):
                msg = "For transform(%s), rule lifetime (%s seconds) >= max_waiting_time(%s)" % (transform['transform_id'],
                                                                                                 self.get_rule_lifetime(rule),
                                                                                                 self.get_max_waiting_time(transform))
                msg += "should create new rule"
                self.logger.info(msg)
                return True
        return False

    def create_new_rule(self, rule, dids, dest_rse, src_rse=None):
        try:
            if 'rule_creator' not in self.plugins:
                # raise exceptions.AgentPluginError('Plugin rule_creator is required')
                err_msg = "Plugin rule_creator is required"
                self.logger.error(err_msg)
                return None

            if not rule:
                return None

            dataset_scope = rule['scope']
            dataset_name = rule['name'] + ".idds_sub_%s" % int(time.time())
            if not src_rse:
                old_src_rse = rule['source_replica_expression']
                if old_src_rse:
                    src_rse = "*\(%s)" % old_src_rse  # noqa: W605
            self.logger.info("Creating new rule for dataset %s:%s, src_rse_expression: %s" % (dataset_scope, dataset_name, src_rse))

            rule_id = self.plugins['rule_creator'](dataset_scope=dataset_scope,
                                                   dataset_name=dataset_name,
                                                   dids=dids,
                                                   dest_rse=dest_rse,
                                                   src_rse=src_rse,
                                                   lifetime=self.new_rule_lifetime)
            return rule_id
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
        return None

    def get_replica_status(self, file_key, replicases_status, new_replicases_statuses):
        for repli_status in [replicases_status] + new_replicases_statuses:
            if file_key in repli_status and repli_status[file_key] in [ContentStatus.Available, ContentStatus.Available.value]:
                return ContentStatus.Available
        return ContentStatus.New

    def __call__(self, processing, transform, input_collection, output_collection, output_contents):
        try:
            processing_metadata = processing['processing_metadata']
            all_rule_notfound = True

            if 'rule_poller' not in self.plugins:
                raise exceptions.AgentPluginError('Plugin rule_poller is required')

            rule_id = processing_metadata['rule_id']
            try:
                basic_rule, basic_replicases_status = self.plugins['rule_poller'](rule_id)
                all_rule_notfound = False
            except exceptions.ProcessNotFound as ex:
                self.logger.warn(ex)
                basic_rule = None
                basic_replicases_status = []

            if 'new_rule_ids' in processing_metadata:
                new_rule_ids = processing_metadata['new_rule_ids']
            else:
                new_rule_ids = []
            new_rules, new_replicases_statuses = [], []
            for rule_id in new_rule_ids:
                try:
                    new_rule, new_replicases_status = self.plugins['rule_poller'](rule_id)
                    all_rule_notfound = False
                except exceptions.ProcessNotFound as ex:
                    self.logger.warn(ex)
                    new_rule, new_replicases_status = None, []
                new_rules.append(new_rule)
                new_replicases_statuses.append(new_replicases_status)

            remain_files = []
            updated_files = []
            processing_updates = {}
            file_status_statistics = {}
            for file in output_contents:
                file_key = '%s:%s' % (file['scope'], file['name'])
                new_file_status = self.get_replica_status(file_key, basic_replicases_status, new_replicases_statuses)

                if not new_file_status == file['status']:
                    file['status'] = new_file_status

                    updated_file = {'content_id': file['content_id'],
                                    'status': new_file_status,
                                    'scope': file['scope'],
                                    'name': file['name'],
                                    'path': None}
                    updated_files.append(updated_file)

                if file['status'] in [ContentStatus.New]:
                    remain_file = {'scope': file['scope'], 'name': file['name']}
                    remain_files.append(remain_file)

                if file['status'] not in file_status_statistics:
                    file_status_statistics[file['status']] = 0
                file_status_statistics[file['status']] += 1

            file_status_keys = list(file_status_statistics.keys())
            if len(file_status_keys) == 1:
                if file_status_keys == [ContentStatus.Available]:
                    processing_status = ProcessingStatus.Finished
                elif file_status_keys == [ContentStatus.Failed]:
                    processing_status = ProcessingStatus.Failed
            else:
                processing_status = ProcessingStatus.Running

            file_statusvalue_statistics = {}
            for key in file_status_statistics:
                file_statusvalue_statistics[key.name] = file_status_statistics[key]
            processing_metadata['content_status_statistics'] = file_statusvalue_statistics

            new_rule_id = None
            # self.logger.info("number of remain files: %s" % len(remain_files))
            if remain_files and self.should_create_new_rule(basic_rule, new_rules, transform):
                self.logger.info("creating new rules")
                new_rule_id = self.create_new_rule(rule=basic_rule, dids=remain_files, dest_rse=basic_rule['rse_expression'])
                self.logger.info("For transform(%s), new rule id: %s" % (transform['transform_id'], new_rule_id))
            if new_rule_id is not None:
                if ('new_rule_ids' not in processing_metadata):
                    processing_metadata['new_rule_ids'] = [new_rule_id]
                else:
                    processing_metadata['new_rule_ids'].append(new_rule_id)

            if all_rule_notfound and new_rule_id is None:
                processing_status = ProcessingStatus.Failed

            processing_updates = {'status': processing_status,
                                  'next_poll_at': datetime.datetime.utcnow() + datetime.timedelta(seconds=self.poll_time_period),
                                  'processing_metadata': processing_metadata}

            return {'updated_files': updated_files, 'processing_updates': processing_updates}
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
