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

import json
import traceback


from idds.common import exceptions
from idds.common.constants import ContentType, ContentStatus
from idds.atlas.transformer.base_plugin import TransformerPluginBase


class HyperParameterOptTransformer(TransformerPluginBase):
    def __init__(self, **kwargs):
        super(HyperParameterOptTransformer, self).__init__(**kwargs)

    def __call__(self, transform, input_collection, output_collection, input_contents):
        try:
            if not input_contents:
                return []

            transform_metadata = transform['transform_metadata']
            initial_points = []
            if 'initial_points' in transform_metadata:
                initial_points = transform_metadata['initial_points']

            output_contents = []
            i = 0
            for initial_point in initial_points:
                point, idds_output = initial_point

                content_metadata = {'input_collection_id': input_collection['coll_id']
                                    }
                content = {'coll_id': output_collection['coll_id'],
                           # 'scope': output_collection['scope'],
                           'scope': 'hpo',
                           'name': str(i),
                           'min_id': 0,
                           'max_id': 0,
                           'status': ContentStatus.New,
                           'path': json.dumps((point, idds_output)),
                           'content_type': ContentType.PseudoContent,
                           'content_metadata': content_metadata}
                if idds_output:
                    content['status'] = ContentStatus.Available
                output_contents.append(content)
                i += 1
            return output_contents
        except Exception as ex:
            self.logger.error(ex)
            self.logger.error(traceback.format_exc())
            raise exceptions.AgentPluginError('%s: %s' % (str(ex), traceback.format_exc()))
