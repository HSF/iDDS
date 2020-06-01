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


from idds.common import exceptions
from idds.common.constants import ContentStatus
from idds.atlas.transformer.base_plugin import TransformerPluginBase


class StageInTransformer(TransformerPluginBase):
    def __init__(self, **kwargs):
        super(StageInTransformer, self).__init__(**kwargs)

    def __call__(self, transform, input_collection, output_collection, input_contents):
        try:
            output_contents = []
            for input_content in input_contents:
                content_metadata = {'input_collection_id': input_collection['coll_id'],
                                    'input_contents': [{'content_id': input_content['content_id'],
                                                        'min_id': input_content['min_id'],
                                                        'max_id': input_content['max_id']}]}
                content = {'coll_id': output_collection['coll_id'],
                           'scope': input_content['scope'],
                           'name': input_content['name'],
                           'min_id': input_content['min_id'],
                           'max_id': input_content['max_id'],
                           'status': ContentStatus.New,
                           'path': None,
                           'content_type': input_content['content_type'],
                           'adler32': input_content['adler32'],
                           'content_metadata': content_metadata}
                output_contents.append(content)
            return output_contents
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
