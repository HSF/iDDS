iDDS RESTful client: Examples
=============================

iDDS provides RESTful services and the client is used to access the RESTful service.

iDDS workflow manager
~~~~~~~~~~~~~~~~~~~~~~~~

1. submit a workflow to the idds server

Below is one example for submitting a workflow.

.. code-block:: python

    from idds.client.clientmanager import ClientManager
    from idds.common.utils import get_rest_host

    # get the host from the client cfg
    host = get_rest_host()
    # or for example, host = https://iddsserver.cern.ch:443/idds

    # get a workflow
    workflow = get_workflow()

    cm = ClientManager(host=host)
    request_id = cm.submit(workflow)

Below is an example for data carousel

.. code-block:: python

    def get_workflow():
        from idds.workflow.workflow import Workflow
        from idds.atlas.workflow.atlasstageinwork import ATLASStageinWork

        scope = 'data16_13TeV'
        name = 'data16_13TeV.00298862.physics_Main.daq.RAW'
        src_rse = 'NDGF-T1_DATATAPE'
        dest_rse = 'NDGF-T1_DATADISK'
        rule_id = '*****'
        workload_id = <panda task id for example>
        work = ATLASStageinWork(primary_input_collection={'scope': scope, 'name': name},
                                output_collections={'scope': scope, 'name': name + '.idds.stagein'},
                                max_waiting_time=max_waiting_time,
                                src_rse=src_rse,
                                dest_rse=dest_rse,
                                rule_id=rule_id)
        wf = Workflow()
        wf.set_workload_id(workload_id)
        wf.add_work(work)
        return wf

Below is an example for hyperparameter optimization

.. code-block:: python

    def get_workflow():
        from idds.workflow.workflow import Workflow
        from idds.atlas.workflow.atlashpowork import ATLASHPOWork

        # request_metadata for predefined method 'nevergrad'
        request_metadata = {'workload_id': '20525135', 'sandbox': None, 'method': 'nevergrad', 'opt_space': {"A": {"type": "Choice", "params": {"choices": [1, 4]}}, "B": {"type": "Scalar", "bounds": [0, 5]}}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 20, 'num_points_per_generation': 10}

        # request_metadata for docker method
        request_metadata = {'workload_id': '20525134', 'sandbox': 'wguanicedew/idds_hpo_nevergrad', 'workdir': '/data', 'executable': 'docker', 'arguments': 'python /opt/hyperparameteropt_nevergrad.py --max_points=%MAX_POINTS --num_points=%NUM_POINTS --input=/data/%IN --output=/data/%OUT', 'output_json': 'output.json', 'opt_space': {"A": {"type": "Choice", "params": {"choices": [1, 4]}}, "B": {"type": "Scalar", "bounds": [0, 5]}}, 'initial_points': [({'A': 1, 'B': 2}, 0.3), ({'A': 1, 'B': 3}, None)], 'max_points': 20, 'num_points_per_generation': 10}

        work = ATLASHPOWork(executable=request_metadata.get('executable', None),
                            arguments=request_metadata.get('arguments', None),
                            parameters=request_metadata.get('parameters', None),
                            setup=None, exec_type='local',
                            sandbox=request_metadata.get('sandbox', None),
                            method=request_metadata.get('method', None),
                            container_workdir=request_metadata.get('workdir', None),
                            output_json=request_metadata.get('output_json', None),
                            opt_space=request_metadata.get('opt_space', None),
                            initial_points=request_metadata.get('initial_points', None),
                            max_points=request_metadata.get('max_points', None),
                            num_points_per_iteration=request_metadata.get('num_points_per_iteration', 10))
        wf = Workflow()
        wf.set_workload_id(request_metadata.get('workload_id', None))
        wf.add_work(work)
        return wf

2. Abort a request

.. code-block:: python

    # One of workload_id or request_id can be None
    clientmanager.abort(request_id=<request_id>, workload_id=<workload_id>)

3. Suspend a request

.. code-block:: python

    # One of workload_id or request_id can be None
    clientmanager.suspend(request_id=<request_id>, workload_id=<workload_id>)

4. Resume a request

.. code-block:: python

    # One of workload_id or request_id can be None
    clientmanager.resume(request_id=<request_id>, workload_id=<workload_id>)

5. Retry a request

.. code-block:: python

    # One of workload_id or request_id can be None
    clientmanager.retry(request_id=<request_id>, workload_id=<workload_id>)

6. Finish a request

.. code-block:: python

    # One of workload_id or request_id can be None
    # if set_all_finished is set, all left files will be set finished
    clientmanager.finish(request_id=<request_id>, workload_id=<workload_id>, set_all_finished=False)

7. Get progress report

.. code-block:: python
       
    # One of workload_id or request_id can be None
    clientmanager.get_status(request_id=<request_id>, workload_id=<workload_id>, with_detail=False/True)

8. Download logs for a request

.. code-block:: python
       
    # One of workload_id or request_id can be None
    clientmanager.download_logs(request_id=<request_id>, workload_id=<workload_id>, dest_dir='./', filename=None)

9. Upload a file to the iDDS cacher

.. code-block:: python

    # filename is the source filename or full path of the source file.
    # Upload file to iDDS cacher: On the cacher, the filename will be the basename of the file.
    clientmanager.upload_to_cacher(filename)

10. Download a file from the iDDS cacher

.. code-block:: python
       
    # filename is the destination filename or full path of the destination file.
    # Download file from iDDS cacher: On the cacher, the filename will be the basename of the file.
    clientmanager.download_from_cacher(filename)

11. Get hyperparameters

.. code-block:: python
       
    clientmanager.get_hyperparameters(request_id=<request_id>, workload_id=<workload_id>,
                                        id=<id>, status=<status>, limit=<limit>)

    from idds.client.clientmanager import ClientManager
    clientmanager = ClientManager(host='https://aipanda160.cern.ch:443/idds')
    clientmanager.get_hyperparameters(workload_id=123, request_id=None)
    clientmanager.get_hyperparameters(workload_id=None, request_id=456)
    clientmanager.get_hyperparameters(workload_id=None, request_id=456, id=0)

12. Update hyperparameter

.. code-block:: python

    clientmanager.update_hyperparameter(request_id=<request_id>, workload_id=<workload_id>,
                                          id=<id>, loss=<loss>)

13. Get messages

.. code-block:: python

    clientmanager.get_messages(request_id=<idds_request_id>, workload_id=<workload_id>)

    from idds.client.clientmanager import ClientManager
    host = 'https://iddsserver.cern.ch:443/idds'
    clientmanager = ClientManager(host=host)

    # clientmanager = ClientManager()  #  if idds.cfg is configured with [rest] host.

    ret = clientmanager.get_messages(request_id=<idds_request_id>)
    ret = clientmanager.get_messages(workload_id=<JEDI_task_id>)
    status, msgs = ret


iDDS Command Line Interface (CLI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Abort a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds abort_requests --request_id=<request_id> --workload_id=<workload_id>

2. Suspend a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds suspend_requests --request_id=<request_id> --workload_id=<workload_id>

3. Resume a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds resume_requests --request_id=<request_id> --workload_id=<workload_id>

4. Retry a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds retry_requests --request_id=<request_id> --workload_id=<workload_id>

5. Finish a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds finish_requests --request_id=<request_id> --workload_id=<workload_id> [--set_all_finished]

6. Get progress report

.. code-block:: python

    # One of workload_id or request_id can be None
    idds get_requests_status --request_id=<request_id> --workload_id=<workload_id> --with_detail

    # idds get_requests_status --request_id 94
    request_id    request_workload_id    scope:name                                status    errors
    ------------  ---------------------  ----------------------------------------  --------  -----------
              94             1616422511  pseudo_dataset:pseudo_input_collection#1  Finished  {'msg': ''}

    # idds get_requests_status --request_id 94 --with_detail
    request_id    transform_id    request_workload_id    transform_workload_id    scope:name                                 status[Total/OK/Processing]    errors
    ------------  --------------  ---------------------  -----------------------  -----------------------------------------  -----------------------------  -----------
              94             151             1616422511                     1003  pseudo_dataset:pseudo_output_collection#1  Finished[6/6/0]                {'msg': ''}
              94             152             1616422511                     1002  pseudo_dataset:pseudo_output_collection#2  Finished[3/3/0]                {'msg': ''}
              94             153             1616422511                     1001  pseudo_dataset:pseudo_output_collection#3  Finished[5/5/0]                {'msg': ''}

7. Download logs for a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds download_logs --request_id=<request_id> --workload_id=<workload_id> --dest_dir='./' --filename=<filename>

8. Upload a file to the iDDS cacher

.. code-block:: python

    # filename is the source filename or full path of the source file.
    # Upload file to iDDS cacher: On the cacher, the filename will be the basename of the file.
    idds upload_to_cacher --filename=<filename>

9. Download a file from the iDDS cacher

.. code-block:: python

    # filename is the destination filename or full path of the destination file.
    # Download file from iDDS cacher: On the cacher, the filename will be the basename of the file.
    idds download_from_cacher --filename=<filename>

10. Get hyperparameters

.. code-block:: python

    idds get_hyperparameters --request_id=<request_id> --workload_id=<workload_id>
                             --id=<id> --status=<status> --limit=<limit>)

    idds get_hyperparameters --workload_id=123
    idds get_hyperparameters --request_id=456
    idds get_hyperparameters --request_id=456 --id=0

11. Update hyperparameter

.. code-block:: python

    idds update_hyperparameter --request_id=<request_id> --workload_id=<workload_id>,
                               --id=<id> --loss=<loss>

12. Get messages

.. code-block:: python

    idds get_messsage --request_id=<request_id> --workload_id=<workload_id>

    idds get_messages --request_id=75483
    idds get_messages --workload_id=25792557
