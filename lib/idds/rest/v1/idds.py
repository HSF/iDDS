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


from flask import Flask

from idds.rest.v1import requests


def get_blueprints():
    bps = []
    bps.append(requests.get_blueprint())


def create_app():
    bps = get_blueprints()

    application = Flask(__name__)
    for bp in bps:
        application.register_blueprint(bp)

    # application.before_request(before_request)
    # application.after_request(after_request)
    return application


application = create_app()
application.run()

