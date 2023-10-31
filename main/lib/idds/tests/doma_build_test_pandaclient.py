#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2022 - 2023


"""
Test client.
"""

import os
import tarfile
import time
import uuid

# from pandaclient import Client

# import traceback

# from rucio.client.client import Client as Rucio_Client
# from rucio.common.exception import CannotAuthenticate

# from idds.client.client import Client
# from idds.client.clientmanager import ClientManager
# from idds.common.constants import RequestType, RequestStatus
# from idds.common.utils import get_rest_host
# from idds.tests.common import get_example_real_tape_stagein_request
# from idds.tests.common import get_example_prodsys2_tape_stagein_request

# from idds.workflowv2.work import Work, Parameter, WorkStatus
# from idds.workflowv2.workflow import Condition, Workflow
from idds.workflowv2.workflow import Workflow
# from idds.atlas.workflowv2.atlasstageinwork import ATLASStageinWork
from idds.doma.workflowv2.domapandawork import DomaPanDAWork

import idds.common.utils as idds_utils
import pandaclient.idds_api


# task_cloud = 'LSST'
task_cloud = 'US'

task_queue = 'DOMA_LSST_GOOGLE_TEST'
# task_queue = 'DOMA_LSST_GOOGLE_MERGE'
# task_queue = 'SLAC_TEST'
# task_queue = 'DOMA_LSST_SLAC_TEST'
task_queue = 'SLAC_Rubin'
# task_queue = 'CC-IN2P3_TEST'


os.environ['PANDA_URL'] = 'http://pandaserver-doma.cern.ch:25080/server/panda'
os.environ['PANDA_URL_SSL'] = 'https://pandaserver-doma.cern.ch:25443/server/panda'
os.environ["PANDACACHE_URL"] = os.environ["PANDA_URL_SSL"]


def get_local_pfns():
    working_dir = os.path.dirname(os.path.realpath(__file__))
    local_pfns = []
    files = ['test_domapanda.py',
             'test_domapanda_build.py',
             'test_domapanda_build_pandaclient.py']
    for f in files:
        full_filename = os.path.join(working_dir, f)
        if os.path.exists(full_filename):
            print("Adding: %s" % full_filename)
            local_pfns.append(full_filename)
        else:
            print("Not exist: %s" % full_filename)
    # add iDDS client code, to be compiled on worker node.
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(working_dir))))
    for idds_dir in ['client', 'common', 'workflow', 'doma']:
        idds_full_dir = os.path.join(base_dir, idds_dir)
        local_pfns.append(idds_full_dir)
    # add main test script
    cmd_build = os.path.join(base_dir, 'main/tools/rubin/cmdline_builder.py')
    local_pfns.append(cmd_build)
    main_script = os.path.join(base_dir, 'main/tools/rubin/test_build.sh')
    local_pfns.append(main_script)
    return local_pfns


def create_archive_file(output_filename, local_pfns):
    if not output_filename.startswith("/"):
        output_filename = os.path.join('/tmp/wguan', output_filename)

    with tarfile.open(output_filename, "w:gz", dereference=True) as tar:
        for local_pfn in local_pfns:
            base_name = os.path.basename(local_pfn)
            tar.add(local_pfn, arcname=os.path.basename(base_name))
    return output_filename


def copy_files_to_pandacache(filename):
    from pandaclient import Client
    status, out = Client.putFile(filename, True)
    print("copy_files_to_pandacache: status: %s, out: %s" % (status, out))
    if out.startswith("NewFileName:"):
        # found the same input sandbox to reuse
        filename = out.split(":")[-1]
    elif out != "True":
        print(out)
        return None

    filename = os.path.basename(filename)
    cache_path = os.path.join(os.environ["PANDACACHE_URL"], 'cache')
    filename = os.path.join(cache_path, filename)
    return filename


def setup_workflow():
    local_pfns = get_local_pfns()
    output_filename = "jobO.%s.tar.gz" % str(uuid.uuid4())
    output_filename = create_archive_file(output_filename, local_pfns)
    print("archive file: %s" % output_filename)
    output_filename = copy_files_to_pandacache(output_filename)
    print("pandacache file: %s" % output_filename)

    # executable = 'unset PYTHONPATH; source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2022_53/loadLSST.bash; pwd; ls -al; setup lsst_distrib;\n python3 ${CTRL_BPS_PANDA_DIR}/python/lsst/ctrl/bps/panda/edgenode/cmd_line_decoder.py'

    executable = 'unset PYTHONPATH; source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2023_41/loadLSST.bash; pwd; ls -al; setup lsst_distrib;'

    # executable += 'if [[ ! -z "${PANDA_AUTH_DIR}" ]] && [[ ! -z "${PANDA_AUTH_ORIGIN}" ]]; then export PANDA_AUTH_ID_TOKEN=$(cat $PANDA_AUTH_DIR); export PANDA_AUTH_VO=$PANDA_AUTH_ORIGIN; export IDDS_OIDC_TOKEN=$(cat $PANDA_AUTH_DIR); export IDDS_VO=$PANDA_AUTH_ORIGIN; export PANDA_AUTH=oidc; else unset PANDA_AUTH; export IDDS_AUTH_TYPE=x509_proxy; fi;'   # noqa E501

    executable += """if [[ ! -z "${PANDA_AUTH_DIR}" ]] && [[ ! -z "${PANDA_AUTH_ORIGIN}" ]];
        then export PANDA_AUTH_ID_TOKEN=$(cat $PANDA_AUTH_DIR);
        export PANDA_AUTH_VO=$PANDA_AUTH_ORIGIN;
        export IDDS_OIDC_TOKEN=$(cat $PANDA_AUTH_DIR);
        export IDDS_VO=$PANDA_AUTH_ORIGIN;
        export PANDA_AUTH=oidc;
        else unset PANDA_AUTH;
        export IDDS_AUTH_TYPE=x509_proxy; fi;
        export PANDA_CONFIG_ROOT=$(pwd);
        export PANDA_VERIFY_HOST=off;
        export PANDA_SYS=$CONDA_PREFIX;
        export PANDA_URL_SSL=${PANDA_SERVER_URL}/server/panda;
        export PANDACACHE_URL=$PANDA_URL_SSL;
        export PANDA_URL=$PANDA_URL_SSL;
        export PANDA_BEHIND_REAL_LB=true;
        """
    # executable = 'python3 ${CTRL_BPS_PANDA_DIR}/python/lsst/ctrl/bps/panda/edgenode/cmd_line_decoder_build.py'
    # executable += 'wget https://storage.googleapis.com/drp-us-central1-containers/cmdline_builder.py;'
    executable += 'wget https://wguan-wisc.web.cern.ch/wguan-wisc/cmdline_builder.py;'
    executable += 'export IDDS_HOST=https://aipanda015.cern.ch:443/idds;'
    executable += 'python3 cmdline_builder.py '

    executable += output_filename + ' ' + './test_build.sh'

    work1 = DomaPanDAWork(executable=executable,
                          task_type='lsst_build',
                          primary_input_collection={'scope': 'pseudo_dataset', 'name': 'pseudo_input_collection#1'},
                          output_collections=[{'scope': 'pseudo_dataset', 'name': 'pseudo_output_collection#1'}],
                          log_collections=[], dependency_map=None,
                          task_name="build_task", task_queue=task_queue,
                          encode_command_line=True,
                          prodSourceLabel='managed',
                          task_log={"dataset": "PandaJob_#{pandaid}/",
                                    "destination": "local",
                                    "param_type": "log",
                                    "token": "local",
                                    "type": "template",
                                    "value": "log.tgz"},
                          task_cloud=task_cloud)

    pending_time = 12
    # pending_time = None
    workflow = Workflow(pending_time=pending_time)

    workflow.add_work(work1)
    workflow.name = 'test_workflow.idds.%s.test' % time.time()
    return workflow


def submit(workflow, idds_server=None):

    c = pandaclient.idds_api.get_api(idds_utils.json_dumps,
                                     idds_host=idds_server, compress=True, manager=True)
    request_id = c.submit_build(workflow, use_dataset_name=False)
    print("Submitted into iDDs with request id=%s", str(request_id))


if __name__ == '__main__':
    workflow = setup_workflow()

    submit(workflow)
