HyperParameterOptimization(HPO)
===============================

HPO is an usecase of iDDS. The purpose of iDDS HPO is to use iDDS to generate hyper parameters for machine learning and trigger production system to automatically process the machine learning training with provided hyper parameters.

iDDS HPO workflow
^^^^^^^^^^^^^^^^^

1. User creates a HPO request through iDDS RESTful service.
2. iDDS HPO generates multiple groups of hyper parameters and stores them in iDDS. Here one group of hyper parameter corresponds a machine learning training job.
3. User gets the hyper parameters from iDDS. Here there are two ways to get the hyper parameters.

    a. For new hyper parameters, iDDS will send a message to ActiveMQ. In this way, production system such as ATLAS JEDI/PanDA can consume these messages to generate a job for every group of hyper parameters.
    b. iDDS also provides RESTful services and client to retrieve the hyper parameters. Users can use the client to retrieve new hyper parameters and generate machine learning training jobs.

4. User finishes the machine learning job and registers the loss of a group of hyper parameters. iDDS provides the RESTful services and client to register the loss.
5. When the number of left unprocessed hyper parameters is below than a threshold, iDDS will automatically generate new groups of hyper parameters. This step will continue until:

    a. max_points is reached. The total number of groups of hyper parameters reaches this number. This parameter can be defined in the request.
    b. The hyper parameter generate process fails or returns [].


Hyper Parameter Generation
--------------------------

Currently iDDS support several different ways to generate hyper parameters.
    1. bayesian: This is a predefined method in iDDS.

        a. The process to generate new hyper parameters: atlas/lib/idds/atlas/processing/hyperparameteropt_bayesian.py
        b. The example code to generate requests: main/lib/idds/tests/hyperparameteropt_bayesian_test.py

    2. nevergrad: This is another predefined method in iDDS.

        a. The process to generate new hyper parameters: atlas/lib/idds/atlas/processing/hyperparameteropt_nevergrad.py
        b. The example code to generate requests: main/lib/idds/tests/hyperparameteropt_nevergrad_test.py

    3. container: Users can also define his own hyper parameter generator with container.

        b. Here is a docker example: main/lib/idds/tests/hyperparameteropt_docker_test.py


RESTful Service
----------------

1. To retrieve hyper parameters.

    client.get_hyperparameters(request_id, status=None, limit=None)

2. To register loss of a group of hyper parameters.

    client.update_hyperparameter(request_id, id, loss)

3. Example code: main/lib/idds/tests/hyperparameteropt_client_test.py


User defied HPO Generator
----------------------

For iDDS HPO, users can define their own HPO generator through docker. When iDDS calls these generators, iDDS will provide these parameters:

    a. '%MAX_POINTS': max number of groups of hyper parameters. iDDS will not stop generating new hyper parameters until it receives an output []. So the generator needs to take care of the max number of points.
    b. '%NUM_POINTS': The number of groups of hyper parameters per generation or per iteration. The default is 10. Users can also update it in the request with 'num_points_per_generation' in the request_metadata.
    c. '%IN': Every time when iDDS calls the generator, iDDS will generate a json file 'idds_input.json' in the current working directory which includes all points(one point is a group of hyper parameter) with corresponding loss or None(if loss is not available). When using docker, users need to mount the current directory and tell the generator the file path in container.
    d. '%OUT': The generator needs to create a an output json file which includes all new points. iDDS will read this file to parse all points. The 'OUT' file name is defined in the 'output_json' in the request_metadata.
    e. input_idds.json: This a file created by iDDS as an input for %IN. Here is one example of idds_input.json(main/lib/idds/tests/idds_input.json). The format of idds_input.json is {"points": [[hyperparameter, loss], [hyperparameter, null]], "opt_space": <opt space>}. 'opt_space' is a copy of the content from your request. If in your request 'opt_space' is not defined, in idds_input.json 'opt_space' will be null(If a hyper parameter is not yet evaluated, the loss will be null(It's None in python dictionary. After json.dumps, it's converted to null). 'points' includes all hyper parameters, whether they are evaluated or not. If a hyper parameter is not yet evaluated, the loss will be null.
    f. How to test the container. Here is one example. Users can update the request part and test their docker locally.
