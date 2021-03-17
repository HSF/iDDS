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

Below is an example for active learning

.. code-block:: python

    from pandatools import Client

    from idds.client.clientmanager import ClientManager
    from idds.common.utils import get_rest_host, run_command
    from idds.workflow.workflow import Condition, Workflow
    from idds.atlas.workflow.atlaspandawork import ATLASPandaWork
    from idds.atlas.workflow.atlasactuatorwork import ATLASActuatorWork

    def get_task_id(output, error):
        m = re.search('jediTaskID=(\d+)', output + error)  # noqa W605
        task_id = int(m.group(1))
        return task_id

    def submit_processing_task():
        cmd = "cd /afs/cern.ch/user/w/wguan/workdisk/iDDS/test/activelearning/hepexcursion/grid; prun --exec 'python simplescript.py 0.5 0.5 200 output.json' --outDS user.wguan.altest123456  --outputs output.json --nJobs=10"
        status, output, error = run_command(cmd)
        if status == 0:
            task_id = get_task_id(output, error)
            return task_id
        else:
            raise Exception(output + error)

    def get_panda_task_paramsmap(panda_task_id):
        status, task_param_map = Client.getTaskParamsMap(self.panda_task_id)
        if status == 0:
            task_param_map = json.loads(task_param_map)
            return task_param_map
        return None

    def get_workflow(panda_task_id):
        #######################################
        # Current workflow:
        # 1. a processing task is submitted to panda with prun or pathena(When prun or pathena provides some interface '--generate_task_parameter_map', this part will be changed). It will be the first ATLASPandaWork (the 'work' below). The ATLASPandaWork will use panda API to get the Panda task_parameter_map and keep this task_parameter_map for later task submission.
        # 2. A learning task is defined ATLASActuatorWork (the 'actuator' below). This task will be executed in iDDS local condor cluster.
        # 3. A DAG condition is defined between 'work' -> 'actuator'. In the example below, when the work 'is_finished' return True, 'actuator' will be triggerred to start. (Note: The condition will be checked only when the current work is terminated. The condtion function can be any function in the current work.)
        # 4. A DAG condition is defined between 'actuator' -> 'work'. In the example below, when the 'actuator' is terminated, the condtion function generate_new_task will be called.
        #    In ATLASActuatorWork, one parameter "output_json='merge.json'" is defined. When ATLASActuatorWork finished, iDDS will read this 'output_json' file and use its contents as the input parameter to clone another ATLASPandaWork and submit this new task to Panda. In this example, if 'output_json' is empty, generate_new_task will return False. No new tasks will be triggered.
        #######################################

        # For ATLASPandaWork, there is a function set_parameters.
        # If set_parameters is not called. ATLASPandaWork will just use the current panda_task_id as its task.
        # If set_parameters is called, ATLASPandaWork will use the new parameter to clone a task from the current panda_task_id.
        # for example, when set_paramter({'m1': 0.1, 'm2': 0.2, 'nevents': 300}), new arguments will be generated based on cmd_to_arguments['parameters']. Which will be 'python simplescript.py 0.1 0.2 300'. It will be used to replace the original task arguments cmd_to_arguments['arguments']
        # The current cmd_to_arguments['outDS'] is also required. Because when generating new tasks, iDDS will generate new dataset name to replace this 'outDS'.
        cmd_to_arguments = {'arguments': 'python simplescript.py 0.5 0.5 200',
                            'parameters': 'python simplescript.py {m1} {m2} {nevents}',
                            'outDS': 'user.wguan.altest123456'}
        work = ATLASPandaWork(panda_task_id=panda_task_id, cmd_to_arguments=cmd_to_arguments)

        # initialize_work will be executed only one time. iDDS will called it automatically.
        # However, because here we need to get the output dataset name(work.get_output_collections called below).
        # If not calling this function,  work.get_output_collections will return None.
        work.initialize_work()

        work_output_coll = work.get_output_collections()[0]

        input_coll = {'scope': work_output_coll['scope'],
                      'name': work_output_coll['name'],
                      'coll_metadata': {'force_close': True}}
        output_coll = {'scope': work_output_coll['scope'],
                       'name': work_output_coll['name'] + "." + str(int(time.time()))}

        # How to generate arguments:
        #     arguments = arguments.format(parameters)   # you can call set_parameters to set different parameters.
        # acutator = ATLASActuatorWork(executable='python', arguments='merge.py {output_json} {events} {dataset}/{filename}',
        acutator = ATLASActuatorWork(executable='python', arguments='merge.py {output_json} {events} {dataset}',
                                     parameters={'output_json': 'merge.json',
                                                 'events': 200,
                                                 'dataset': '{scope}:{name}'.format(**input_coll),
                                                 'filename': 'output*.json'},
                                     sandbox=work.sandbox, primary_input_collection=input_coll,
                                     output_collections=output_coll, output_json='merge.json')
        wf = Workflow()
        wf.add_work(work)
        wf.add_work(acutator)
        cond = Condition(work.is_finished, current_work=work, true_work=acutator, false_work=None)
        wf.add_condition(cond)
        cond1 = Condition(acutator.generate_new_task, current_work=acutator, true_work=work, false_work=None)
        wf.add_condition(cond1)

        # because the two works are in a loop, they are not independent. This call is needed to tell which one to start.
        # otherwise idds will use the first one to start.
        wf.add_initial_works(work)

        return wf

2. Abort a request

.. code-block:: python

    # One of workload_id or request_id can be None
    clientmanager.abort(request_id=<request_id>, workload_id=<workload_id>)

3. Get progress report

.. code-block:: python
       
    # One of workload_id or request_id can be None
    clientmanager.get_status(request_id=<request_id>, workload_id=<workload_id>, with_detail=False/True)

4. Download logs for a request

.. code-block:: python
       
    # One of workload_id or request_id can be None
    clientmanager.download_logs(request_id=<request_id>, workload_id=<workload_id>, dest_dir='./', filename=None)

5. Upload a file to the iDDS cacher

.. code-block:: python

    # filename is the source filename or full path of the source file.
    # Upload file to iDDS cacher: On the cacher, the filename will be the basename of the file.
    clientmanager.upload_to_cacher(filename)

6. Download a file from the iDDS cacher

.. code-block:: python
       
    # filename is the destination filename or full path of the destination file.
    # Download file from iDDS cacher: On the cacher, the filename will be the basename of the file.
    clientmanager.download_from_cacher(filename)

7. Get hyperparameters

.. code-block:: python
       
    clientmanager.get_hyperparameters(request_id=<request_id>, workload_id=<workload_id>,
                                        id=<id>, status=<status>, limit=<limit>)

    clientmanager.get_hyperparameters(workload_id=123, request_id=None)
    clientmanager.get_hyperparameters(workload_id=None, request_id=456)
    clientmanager.get_hyperparameters(workload_id=None, request_id=456, id=0)

8. Update hyperparameter

.. code-block:: python

    clientmanager.update_hyperparameter(request_id=<request_id>, workload_id=<workload_id>,
                                          id=<id>, loss=<loss>)

iDDS Command Line Interface (CLI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Abort a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds abort-requests --request_id=<request_id> --workload_id=<workload_id>

2. Get progress report

.. code-block:: python

    # One of workload_id or request_id can be None
    idds get_requests_status --request_id=<request_id> --workload_id=<workload_id> --with_detail=False/True

3. Download logs for a request

.. code-block:: python

    # One of workload_id or request_id can be None
    idds download_logs --request_id=<request_id> --workload_id=<workload_id> --dest_dir='./' --filename=<filename>

4. Upload a file to the iDDS cacher

.. code-block:: python

    # filename is the source filename or full path of the source file.
    # Upload file to iDDS cacher: On the cacher, the filename will be the basename of the file.
    idds upload_to_cacher --filename=<filename>

5. Download a file from the iDDS cacher

.. code-block:: python

    # filename is the destination filename or full path of the destination file.
    # Download file from iDDS cacher: On the cacher, the filename will be the basename of the file.
    idds download_from_cacher --filename=<filename>

6. Get hyperparameters

.. code-block:: python

    idds get_hyperparameters --request_id=<request_id> --workload_id=<workload_id>
                             --id=<id> --status=<status> --limit=<limit>)

    idds get_hyperparameters --workload_id=123
    idds get_hyperparameters --request_id=456
    idds get_hyperparameters --request_id=456 --id=0

7. Update hyperparameter

.. code-block:: python

    idds update_hyperparameter --request_id=<request_id> --workload_id=<workload_id>,
                               --id=<id> --loss=<loss>
