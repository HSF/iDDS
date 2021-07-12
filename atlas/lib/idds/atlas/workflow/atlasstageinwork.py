#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2020

import copy
import datetime
import traceback

from rucio.client.client import Client as RucioClient
from rucio.common.exception import (CannotAuthenticate as RucioCannotAuthenticate,
                                    DuplicateRule as RucioDuplicateRule,
                                    RuleNotFound as RucioRuleNotFound)

from idds.common import exceptions
from idds.common.constants import (TransformType, CollectionType, CollectionStatus,
                                   ContentStatus, ContentType,
                                   ProcessingStatus, WorkStatus)
from idds.workflow.work import Work, Processing


class ATLASStageinWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None,
                 work_tag='stagein', exec_type='local', sandbox=None, work_id=None,
                 primary_input_collection=None, other_input_collections=None,
                 output_collections=None, log_collections=None,
                 agent_attributes=None,
                 logger=None,
                 max_waiting_time=3600 * 7 * 24, src_rse=None, dest_rse=None, rule_id=None):
        """
        Init a work/task/transformation.

        :param setup: A string to setup the executable enviroment, it can be None.
        :param executable: The executable.
        :param arguments: The arguments.
        :param parameters: A dict with arguments needed to be replaced.
        :param work_type: The work type like data carousel, hyperparameteroptimization and so on.
        :param exec_type: The exec type like 'local', 'remote'(with remote_package set), 'docker' and so on.
        :param sandbox: The sandbox.
        :param work_id: The work/task id.
        :param primary_input_collection: The primary input collection.
        :param other_input_collections: List of the input collections.
        :param output_collections: List of the output collections.
        # :param workflow: The workflow the current work belongs to.
        :param max_waiting_time: The max waiting time to terminate the work.
        :param src_rse: The source rse.
        :param dest_rse: The destination rse.
        :param rule_id: The rule id.
        """
        super(ATLASStageinWork, self).__init__(executable=executable, arguments=arguments,
                                               parameters=parameters, setup=setup, work_type=TransformType.StageIn,
                                               exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                               primary_input_collection=primary_input_collection,
                                               other_input_collections=other_input_collections,
                                               output_collections=output_collections,
                                               log_collections=log_collections,
                                               agent_attributes=agent_attributes,
                                               logger=logger)
        self.max_waiting_time = max_waiting_time
        self.src_rse = src_rse
        self.dest_rse = dest_rse
        self.life_time = max_waiting_time
        self.rule_id = rule_id

        self.num_mapped_inputs = 0
        self.total_output_files = 0
        self.processed_output_files = 0
        self.status_statistics = {}

    def get_rucio_client(self):
        try:
            client = RucioClient()
        except RucioCannotAuthenticate as error:
            self.logger.error(error)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(error), traceback.format_exc()))
        return client

    def poll_external_collection(self, coll):
        try:
            # if 'coll_metadata' in coll and 'is_open' in coll['coll_metadata'] and not coll['coll_metadata']['is_open']:
            if coll.status in [CollectionStatus.Closed]:
                return coll
            else:
                client = self.get_rucio_client()
                did_meta = client.get_metadata(scope=coll.scope, name=coll.name)

                coll.coll_metadata['bytes'] = did_meta['bytes']
                coll.coll_metadata['total_files'] = did_meta['length']
                coll.coll_metadata['availability'] = did_meta['availability']
                coll.coll_metadata['events'] = did_meta['events']
                coll.coll_metadata['is_open'] = did_meta['is_open']
                coll.coll_metadata['run_number'] = did_meta['run_number']
                coll.coll_metadata['did_type'] = did_meta['did_type']
                coll.coll_metadata['list_all_files'] = False

                if 'is_open' in coll.coll_metadata and not coll.coll_metadata['is_open']:
                    coll_status = CollectionStatus.Closed
                else:
                    coll_status = CollectionStatus.Open
                coll.status = coll_status

                if 'did_type' in coll.coll_metadata:
                    if coll.coll_metadata['did_type'] == 'DATASET':
                        coll_type = CollectionType.Dataset
                    elif coll.coll_metadata['did_type'] == 'CONTAINER':
                        coll_type = CollectionType.Container
                    else:
                        coll_type = CollectionType.File
                else:
                    coll_type = CollectionType.Dataset
                coll.coll_metadata['coll_type'] = coll_type

                return coll
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_input_collections(self):
        # return [self.primary_input_collection] + self.other_input_collections
        colls = [self.primary_input_collection] + self.other_input_collections
        for coll_int_id in colls:
            coll = self.collections[coll_int_id]
            coll = self.poll_external_collection(coll)
            self.collections[coll_int_id] = coll
        return super(ATLASStageinWork, self).get_input_collections()

    def get_input_contents(self):
        """
        Get all input contents from DDM.
        """
        try:
            ret_files = []
            rucio_client = self.get_rucio_client()
            files = rucio_client.list_files(scope=self.collections[self.primary_input_collection].scope,
                                            name=self.collections[self.primary_input_collection].name)
            for file in files:
                ret_file = {'coll_id': self.collections[self.primary_input_collection].coll_id,
                            'scope': file['scope'],
                            'name': file['name'],
                            'bytes': file['bytes'],
                            'adler32': file['adler32'],
                            'min_id': 0,
                            'max_id': file['events'],
                            'content_type': ContentType.File,
                            'content_metadata': {'events': file['events']}}
                ret_files.append(ret_file)
            return ret_files
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.IDDSException('%s: %s' % (str(ex), traceback.format_exc()))

    def get_mapped_inputs(self, mapped_input_output_maps):
        ret = []
        for map_id in mapped_input_output_maps:
            inputs = mapped_input_output_maps[map_id]['inputs']

            # if 'primary' is not set, the first one is the primary input.
            primary_input = inputs[0]
            for ip in inputs:
                if 'primary' in ip['content_metadata'] and ip['content_metadata']['primary']:
                    primary_input = ip
            ret.append(primary_input)
        return ret

    def get_new_input_output_maps(self, mapped_input_output_maps={}):
        """
        New inputs which are not yet mapped to outputs.

        :param mapped_input_output_maps: Inputs that are already mapped.
        """
        inputs = self.get_input_contents()
        mapped_inputs = self.get_mapped_inputs(mapped_input_output_maps)
        mapped_inputs_scope_name = [ip['scope'] + ":" + ip['name'] for ip in mapped_inputs]

        new_inputs = []
        new_input_output_maps = {}
        for ip in inputs:
            ip_scope_name = ip['scope'] + ":" + ip['name']
            if ip_scope_name not in mapped_inputs_scope_name:
                new_inputs.append(ip)

        # to avoid cheking new inputs if there are no new inputs anymore
        if (not new_inputs and self.collections[self.primary_input_collection].status in [CollectionStatus.Closed]):  # noqa: W503
            self.set_has_new_inputs(False)
        else:
            mapped_keys = mapped_input_output_maps.keys()
            if mapped_keys:
                next_key = max(mapped_keys) + 1
            else:
                next_key = 1
            for ip in new_inputs:
                self.num_mapped_inputs += 1
                out_ip = copy.deepcopy(ip)
                ip['status'] = ContentStatus.Available
                ip['substatus'] = ContentStatus.Available
                out_ip['coll_id'] = self.collections[self.output_collections[0]].coll_id
                new_input_output_maps[next_key] = {'inputs': [ip],
                                                   'outputs': [out_ip],
                                                   'inputs_dependency': [],
                                                   'logs': []}
                next_key += 1

        return new_input_output_maps

    def get_processing(self, input_output_maps, without_creating=False):
        if self.active_processings:
            return self.processings[self.active_processings[0]]
        else:
            if not without_creating:
                return self.create_processing(input_output_maps)
        return None

    def create_processing(self, input_output_maps=[]):
        processing_metadata = {'src_rse': self.src_rse,
                               'dest_rse': self.dest_rse,
                               'life_time': self.life_time,
                               'rule_id': self.rule_id}
        proc = Processing(processing_metadata=processing_metadata)
        proc.external_id = self.rule_id
        if self.rule_id:
            proc.submitted_at = datetime.datetime.utcnow()

        self.add_processing_to_processings(proc)
        self.active_processings.append(proc.internal_id)
        return proc

    def create_rule(self, processing):
        try:
            rucio_client = self.get_rucio_client()
            ds_did = {'scope': self.collections[self.primary_input_collection].scope,
                      'name': self.collections[self.primary_input_collection].name}
            rule_id = rucio_client.add_replication_rule(dids=[ds_did],
                                                        copies=1,
                                                        rse_expression=self.dest_rse,
                                                        source_replica_expression=self.src_rse,
                                                        lifetime=self.lifetime,
                                                        locked=False,
                                                        grouping='DATASET',
                                                        ask_approval=False)
            if type(rule_id) in (list, tuple):
                rule_id = rule_id[0]
            return rule_id
        except RucioDuplicateRule as ex:
            self.logger.warn(ex)
            rules = rucio_client.list_did_rules(scope=self.collections[self.primary_input_collection].scope,
                                                name=self.collections[self.primary_input_collection].name)
            for rule in rules:
                if rule['account'] == rucio_client.account and rule['rse_expression'] == self.dest_rse:
                    return rule['id']
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            # raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
        return None

    def submit_processing(self, processing):
        proc = processing['processing_metadata']['processing']
        if proc.external_id:
            # if 'rule_id' in processing['processing_meta']:
            pass
        else:
            rule_id = self.create_rule(processing)
            # processing['processing_metadata']['rule_id'] = rule_id
            proc.external_id = rule_id
            if rule_id:
                proc.submitted_at = datetime.datetime.utcnow()

    def poll_rule(self, processing):
        try:
            # p = processing
            # rule_id = p['processing_metadata']['rule_id']
            proc = processing['processing_metadata']['processing']
            rule_id = proc.external_id

            replicases_status = {}
            if rule_id:
                if not isinstance(rule_id, (tuple, list)):
                    rule_id = [rule_id]

                rucio_client = self.get_rucio_client()
                for rule_id_item in rule_id:
                    rule = rucio_client.get_replication_rule(rule_id=rule_id_item)
                    # rule['state']

                    if rule['locks_ok_cnt'] > 0:
                        locks = rucio_client.list_replica_locks(rule_id=rule_id_item)
                        for lock in locks:
                            scope_name = '%s:%s' % (lock['scope'], lock['name'])
                            if lock['state'] == 'OK':
                                replicases_status[scope_name] = ContentStatus.Available   # 'OK'
            return processing, rule['state'], replicases_status
        except RucioRuleNotFound as ex:
            msg = "rule(%s) not found: %s" % (str(rule_id), str(ex))
            raise exceptions.ProcessNotFound(msg)

    def poll_processing(self, processing):
        return self.poll_rule(processing)

    def poll_processing_updates(self, processing, input_output_maps):
        processing, rule_state, rep_status = self.poll_processing(processing)

        updated_contents = []
        content_substatus = {'finished': 0, 'unfinished': 0}
        for map_id in input_output_maps:
            outputs = input_output_maps[map_id]['outputs']
            for content in outputs:
                key = '%s:%s' % (content['scope'], content['name'])
                if key in rep_status:
                    if content['substatus'] != rep_status[key]:
                        updated_content = {'content_id': content['content_id'],
                                           'substatus': rep_status[key]}
                        updated_contents.append(updated_content)
                        content['substatus'] = rep_status[key]
                if content['substatus'] == ContentStatus.Available:
                    content_substatus['finished'] += 1
                else:
                    content_substatus['unfinished'] += 1

        update_processing = {}
        if rule_state == 'OK' and content_substatus['finished'] > 0 and content_substatus['unfinished'] == 0:
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Finished}}
        elif self.toexpire:
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Expired}}
        elif self.tocancel:
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Cancelled}}
        elif self.tosuspend:
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Suspended}}
        elif self.toresume:
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Running}}
            update_processing['parameters']['expired_at'] = None
            processing['expired_at'] = None
            proc = processing['processing_metadata']['processing']
            proc.has_new_updates()
        elif self.tofinish:
            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.SubFinished}}
        elif self.toforcefinish:
            for map_id in input_output_maps:
                outputs = input_output_maps[map_id]['outputs']
                for content in outputs:
                    if content['substatus'] not in [ContentStatus.Available, ContentStatus.FakeAvailable]:
                        updated_content = {'content_id': content['content_id'],
                                           'substatus': ContentStatus.FakeAvailable}
                        updated_contents.append(updated_content)
                        content['substatus'] = ContentStatus.FakeAvailable

            update_processing = {'processing_id': processing['processing_id'],
                                 'parameters': {'status': ProcessingStatus.Finished}}

        if updated_contents:
            proc = processing['processing_metadata']['processing']
            proc.has_new_updates()

        return update_processing, updated_contents

    def get_status_statistics(self, registered_input_output_maps):
        status_statistics = {}

        self.total_output_files = 0
        self.processed_output_file = 0

        for map_id in registered_input_output_maps:
            # inputs = registered_input_output_maps[map_id]['inputs']
            outputs = registered_input_output_maps[map_id]['outputs']

            self.total_output_files += 1

            for content in outputs:
                if content['status'].name not in status_statistics:
                    status_statistics[content['status'].name] = 0
                status_statistics[content['status'].name] += 1

                if content['status'] == ContentStatus.Available:
                    self.processed_output_file += 1

        self.status_statistics = status_statistics
        return status_statistics

    def syn_work_status(self, registered_input_output_maps, all_updates_flushed=True, output_statistics={}, to_release_input_contents=[]):
        super(ATLASStageinWork, self).syn_work_status(registered_input_output_maps)
        self.get_status_statistics(registered_input_output_maps)

        # self.syn_collection_status()

        self.logger.debug("syn_work_status(%s): is_processings_terminated: %s" % (str(self.get_processing_ids()), str(self.is_processings_terminated())))
        self.logger.debug("syn_work_status(%s): has_new_inputs: %s" % (str(self.get_processing_ids()), str(self.has_new_inputs)))
        if self.is_processings_terminated() and not self.has_new_inputs:
            if not self.is_all_outputs_flushed(registered_input_output_maps):
                self.logger.warn("The processing is terminated. but not all outputs are flushed. Wait to flush the outputs then finish the transform")
                return

            keys = self.status_statistics.keys()
            if len(keys) == 1:
                if ContentStatus.Available.name in keys:
                    self.status = WorkStatus.Finished
                else:
                    self.status = WorkStatus.Failed
            else:
                self.status = WorkStatus.SubFinished
        self.logger.debug("syn_work_status(%s): work.status: %s" % (str(self.get_processing_ids()), str(self.status)))
