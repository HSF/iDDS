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
    from idds.workflowv2.workflow import Workflow, Condition
    from idds.atlas.workflowv2.atlaspandawork import ATLASPandaWork

    task_parameters1 = {"architecture": "",
                        "cliParams": "prun --exec 'echo %RNDM:10 > seed.txt' --outputs seed.txt --nJobs 2 --outDS user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top",
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
                                "value": "-a jobO.185663cd-6df9-4ac8-adf9-0d9bf9d5892e.tar.gz"
                            },
                            {
                                "container": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top_seed.txt/",
                                "dataset": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top_seed.txt/",
                                "hidden": True,
                                "param_type": "output",
                                "type": "template",
                                "value": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top.$JEDITASKID._${SN/P}.seed.txt"
                            },
                            {
                                "type": "constant",
                                "value": "-o \"{'seed.txt': 'user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top.$JEDITASKID._${SN/P}.seed.txt'}\""
                            }
                        ],
                        "log": {
                            "container": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top/",
                            "dataset": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top/",
                            "param_type": "log",
                            "type": "template",
                            "value": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top.log.$JEDITASKID.${SN}.log.tgz"
                        },
                        "nEvents": 2,
                        "nEventsPerJob": 1,
                        "nMaxFilesPerJob": 200,
                        "noInput": True,
                        "osInfo": "Linux-3.10.0-1160.36.2.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
                        "processingType": "panda-client-1.4.80-jedi-run",
                        "prodSourceLabel": "user",
                        "respectSplitRule": True,
                        "site": None,
                        "sourceURL": "https://aipanda048.cern.ch:25443",
                        "taskName": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top",
                        "transHome": None,
                        "transUses": "",
                        "uniqueTaskName": True,
                        "userName": "Tadashi Maeno",
                        "vo": "atlas"}

    task_parameters2 = {"architecture": "",
                        "cliParams": "prun --exec 'echo %IN > results.root' --outputs results.root --forceStaged --inDS user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top_seed.txt/ --outDS user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom",
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
                                "value": "echo%20%25IN%20%3E%20results.root"
                            },
                            {
                                "type": "constant",
                                "value": "\""
                            },
                            {
                                "type": "constant",
                                "value": "-a jobO.185663cd-6df9-4ac8-adf9-0d9bf9d5892e.tar.gz"
                            },
                            {
                                "dataset": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top_seed.txt/",
                                "exclude": "\\.log\\.tgz(\\.\\d+)*$",
                                "expand": True,
                                "param_type": "input",
                                "type": "template",
                                "value": "-i \"${IN/T}\""
                            },
                            {
                                "container": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom_results.root/",
                                "dataset": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom_results.root/",
                                "hidden": True,
                                "param_type": "output",
                                "type": "template",
                                "value": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom.$JEDITASKID._${SN/P}.results.root"
                            },
                            {
                                "type": "constant",
                                "value": "-o \"{'results.root': 'user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom.$JEDITASKID._${SN/P}.results.root'}\""
                            }
                        ],
                        "log": {
                            "container": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom/",
                            "dataset": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom/",
                            "param_type": "log",
                            "type": "template",
                            "value": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom.log.$JEDITASKID.${SN}.log.tgz"
                        },
                        "nMaxFilesPerJob": 200,
                        "noWaitParent": True,
                        "osInfo": "Linux-3.10.0-1160.36.2.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
                        "parentTaskName": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_000_top",
                        "processingType": "panda-client-1.4.80-jedi-run",
                        "prodSourceLabel": "user",
                        "respectSplitRule": True,
                        "site": None,
                        "sourceURL": "https://aipanda048.cern.ch:25443",
                        "taskName": "user.tmaeno.389eb4c5-5db6-4b80-82aa-9edfae6dfb38_001_bottom",
                        "transHome": None,
                        "transUses": "",
                        "uniqueTaskName": True,
                        "userName": "Tadashi Maeno",
                        "vo": "atlas"}

    work1 = ATLASPandaWork(task_parameters=task_parameters1)
    work2 = ATLASPandaWork(task_parameters=task_parameters2)
    wf = Workflow()
    wf.set_workload_id(234567)
    wf.add_work(work1)
    wf.add_work(work2)

    # cond = Condition(cond=work1.is_finished, true_work=work2)
    cond = Condition(cond=work1.is_started, true_work=work2)
    wf.add_condition(cond)
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
