#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

"""----------------------
   Web service startup
----------------------"""

import os
import pprint
os.environ['IDDS_CONFIG'] = '/opt/idds/etc/idds/idds.cfg'  # noqa: E402

from idds.rest.v1.app import create_app    # noqa E402

application = create_app()


@application.route('/', defaults={'path': ''})
@application.route('/<path:path>')
def catchall(path):
    application.logger.info(pprint.pprint(application.get_request()))
    application.logger.info(pprint.pprint(application.get_request().endpoint))
    application.logger.info(pprint.pprint(application.get_request().url_rule))
