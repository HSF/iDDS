#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019 - 2021


"""
Test client.
"""


from idds.client.clientmanager import ClientManager
from idds.common.utils import get_rest_host


def get_workflow():
    from idds.workflow.workflow import Workflow
    from idds.atlas.workflow.atlaspandawork import ATLASPandaWork

    task_parameters = {"architecture": "",
                       "cliParams": "prun --exec 'echo %RNDM:10 > seed.txt' --outputs seed.txt --nJobs 3 --outDS user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top",
                       "excludedSite": [],
                       "includedSite": None,
                       "jobParameters": [
                           {
                               "type": "constant",
                               "value": "-j \"\" --sourceURL ${SURL}"
                           },
                           {
                               "type": "constant",
                               "value": "-r ."
                           },
                           {
                               "padding": False,
                               "type": "constant",
                               "value": "-p \""
                           },
                           {
                               "padding": False,
                               "type": "constant",
                               "value": "echo%20%25RNDM%3A10%20%3E%20seed.txt"
                           },
                           {
                               "type": "constant",
                               "value": "\""
                           },
                           {
                               "type": "constant",
                               "value": "-a jobO.6f1dc5a8-eeb3-4cdf-aed0-0930b6a0e815.tar.gz"
                           },
                           [
                               {
                                   "container": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top_seed.txt/",
                                   "dataset": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top_seed.txt/",
                                   "hidden": True,
                                   "param_type": "output",
                                   "type": "template",
                                   "value": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top.$JEDITASKID._${SN/P}.seed.txt"
                               }
                           ],
                           {
                               "type": "constant",
                               "value": "-o \"{'seed.txt': 'user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top.$JEDITASKID._${SN/P}.seed.txt'}\""
                           }
                       ],
                       "log": {
                           "container": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top/",
                           "dataset": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top/",
                           "param_type": "log",
                           "type": "template",
                           "value": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top.log.$JEDITASKID.${SN}.log.tgz"
                       },
                       "nEvents": 3,
                       "nEventsPerJob": 1,
                       "nMaxFilesPerJob": 200,
                       "noInput": True,
                       "osInfo": "Linux-3.10.0-1160.31.1.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
                       "processingType": "panda-client-1.4.79-jedi-run",
                       "prodSourceLabel": "user",
                       "respectSplitRule": True,
                       "site": None,
                       "sourceURL": "https://aipanda059.cern.ch:25443",
                       "taskName": "user.tmaeno.34ba141d-242c-43cf-a326-7730a5dc7338_000_top",
                       "transHome": None,
                       "transUses": "",
                       "uniqueTaskName": True,
                       "userName": "Tadashi Maeno",
                       "vo": "atlas"
                       }

    work = ATLASPandaWork(task_parameters=task_parameters)
    wf = Workflow()
    wf.set_workload_id(234567)
    wf.add_work(work)
    return wf


if __name__ == '__main__':
    host = get_rest_host()
    # props = get_req_properties()
    # props = get_example_real_tape_stagein_request()
    # props = get_example_prodsys2_tape_stagein_request()
    # props = get_example_active_learning_request()
    workflow = get_workflow()

    # props = pre_check(props)
    # print(props)

    wm = ClientManager(host=host)
    request_id = wm.submit(workflow)
    print(request_id)
