iDDS RESTful client: Examples
=============================

iDDS provides RESTful services and the client is used to access the RESTful service.

iDDS request client
~~~~~~~~~~~~~~~~~~~

1. request properties

Below is one example for data carousel request.

.. code-block:: python

    req_properties = {
        'scope': 'data16_13TeV',
        'name': 'data16_13TeV.00298862.physics_Main.daq.RAW',
        'requester': 'panda',
        'request_type': RequestType.StageIn,
        'transform_tag': 's2395',
        'status': RequestStatus.New,
        'priority': 0,
        'lifetime': 30,
        'request_metadata': {'workload_id': '20776840', 'src_rse': 'NDGF-T1_DATATAPE', 'dest_rse': 'NDGF-T1_DATADISK', 'rule_id': '236e4bf87e11490291e3259b14724e30'}
    }

2. add request

.. code-block:: python

    request_id = client.add_request(**props)

3. Example codes:

.. code-block:: python

    main/lib/idds/tests/datacarousel_test.py,
    main/lib/idds/tests/activelearning_test.py
    main/lib/idds/tests/hyperparameteropt_bayesian_test.py
    main/lib/idds/tests/hyperparameteropt_docker_local_test.py
    main/lib/idds/tests/hyperparameteropt_docker_test.py
    main/lib/idds/tests/hyperparameteropt_nevergrad_test.py

iDDS HPO(HyperParameterClient)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. To retrieve hyperparameters.

.. code-block:: python

    client.get_hyperparameters(workload_id, request_id, id=None, status=None, limit=None)

    examples:
        client.get_hyperparameters(workload_id=123, request_id=None)
        client.get_hyperparameters(workload_id=None, request_id=456)
        client.get_hyperparameters(workload_id=None, request_id=456, id=0)

2. To register loss of a group of hyperparameters.

.. code-block:: python

    client.update_hyperparameter(request_id, id, loss)

3. Example code:

.. code-block:: python

    main/lib/idds/tests/hyperparameteropt_client_test.py

iDDS logs client
~~~~~~~~~~~~~~~~

iDDS also provides a rest service for users to download logs for tasks running on iDDS, for example ActiveLearning and HyperParameterOptimization.

1. Download logs:

.. code-block:: python

    client.download_logs(workload_id=workload_id, request_id=request_id, dest_dir='/tmp')

2. Example codes:

.. code-block:: python

    main/lib/idds/tests/logs_test.py
