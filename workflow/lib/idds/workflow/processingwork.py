#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022

from idds.common.constants import TransformType

from .work import Work


class ProcessingWork(Work):
    def __init__(self, executable=None, arguments=None, parameters=None, setup=None, work_type=None,
                 work_tag=None, exec_type='local', sandbox=None, request_id=None, work_id=None, work_name=None,
                 primary_input_collection=None, other_input_collections=None, input_collections=None,
                 primary_output_collection=None, other_output_collections=None, output_collections=None,
                 log_collections=None, release_inputs_after_submitting=False, username=None,
                 agent_attributes=None, is_template=False,
                 logger=None):
        super(ProcessingWork, self).__init__(executable=executable, arguments=arguments,
                                             parameters=parameters, setup=setup, work_type=TransformType.Processing,
                                             exec_type=exec_type, sandbox=sandbox, work_id=work_id,
                                             primary_input_collection=primary_input_collection,
                                             other_input_collections=other_input_collections,
                                             primary_output_collection=primary_output_collection,
                                             other_output_collections=other_output_collections,
                                             input_collections=input_collections,
                                             output_collections=output_collections,
                                             log_collections=log_collections,
                                             agent_attributes=agent_attributes,
                                             logger=logger)
