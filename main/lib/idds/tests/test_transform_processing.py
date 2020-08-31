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

import unittest2 as unittest
from nose.tools import assert_equal

from idds.common.utils import check_database, has_config, setup_logging
from idds.common.constants import ProcessingStatus
from idds.orm.transforms import add_transform, delete_transform
from idds.orm.processings import (add_processing, update_processing,
                                  get_processing, delete_processing)
from idds.tests.common import get_transform_properties, get_processing_properties

setup_logging(__name__)


class TestTransformProcessing(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_transform_processing_orm(self):
        """ Transform/Processing (ORM): Test the creation, query, and cancel of a Transform/Processing """
        trans_properties = get_transform_properties()

        proc_properties = get_processing_properties()

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

        processing = get_processing(processing_id=processing_id)
        assert_equal(processing, None)

        delete_transform(trans_id)
