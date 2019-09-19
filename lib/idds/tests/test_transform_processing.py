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
Test Request.
"""
import datetime

import unittest2 as unittest
from nose.tools import assert_equal, assert_raises

from idds.common import exceptions
from idds.common.constants import GranularityType, ProcessingStatus, TransformType, TransformStatus
from idds.common.utils import check_database, has_config, setup_logging
from idds.core.transforms import add_transform, delete_transform
from idds.core.processings import (add_processing, update_processing,
                                   get_processing, delete_processing)
setup_logging(__name__)


class TestTransformProcessing(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_transform_processing_core(self):
        """ Transform/Processing (CORE): Test the creation, query, and cancel of a Transform/Processing """
        trans_properties = {
            'transform_type': TransformType.EventStreaming,
            'transform_tag': 's3128',
            'priority': 0,
            'status': TransformStatus.New,
            'retries': 0,
            'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
            'transform_metadata': {'input': {'coll_id': 123},
                                   'output': {'coll_id': 456},
                                   'log': {'coll_id': 789}}
        }

        proc_properties = {
            'transform_id': None,
            'status': ProcessingStatus.New,
            'submitter': 'panda',
            'granularity': 10,
            'granularity_type': GranularityType.File,
            'expired_at': datetime.datetime.utcnow().replace(microsecond=0),
            'processing_metadata': {'task_id': 40191323}
        }

        trans_id = add_transform(**trans_properties)

        proc_properties['transform_id'] = trans_id
        processing_id = add_processing(**proc_properties)
        processing = get_processing(processing_id=processing_id)
        for key in proc_properties:
            assert_equal(processing[key], proc_properties[key])

        update_processing(processing_id, {'status': ProcessingStatus.Failed})
        processing = get_processing(processing_id=processing_id)
        assert_equal(processing['status'], ProcessingStatus.Failed)

        delete_processing(processing_id)

        with assert_raises(exceptions.NoObject):
            get_processing(processing_id=processing_id)

        delete_transform(trans_id)
