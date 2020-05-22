HyperParameterOptimization(HPO)
===============================

HPO is a usecase of iDDS. The purpose of iDDS HPO is to use iDDS to generate hyperparameters for machine learning and trigger production system to automatically process the machine learning training with provided hyperparameters.

iDDS HPO workflow
^^^^^^^^^^^^^^^^^

1. The user submit a HPO task to JEDI.
2. JEDI submits a HPO request to iDDS.
3. iDDS generates multiple sets of hyperparameters and stores them in iDDS database. Each set of hyperparameters has a serial number and corresponds to a machine learning training job.
4. Serial numbers are sent to JEDI through ActiveMQ.
5. JEDI generates PanDA jobs and dispatches them to pilots running on compute resources.
6. Each PanDA job fetches a serial number from PanDA and converts it to a set of hyperparameters by checking with iDDS.
7. Once the hyperparameter set is evaluated the validation loss is registered to iDDS.
8. The PanDA job fetches another serial number and evaluates corresponding hyperparameter set if wall-time is still available.
9. When the number of unprocessed hyperparameter sets goes below a threshold, iDDS automatically generates new sets of hyperparameters. This step will continue until:

    a. The total number of groups of hyperparameters reaches max_points. This parameter can be defined in the request.
    b. The hyperparameter generate process fails or returns [].

10. When all hyperparameter sets are evaluated iDDS sends a message to JEDI to terminate the HPO task.


Hyperparameter Generation
--------------------------

Currently iDDS support several different ways to generate hyperparameters.
    1. bayesian: This is a predefined method in iDDS.

        a. The process to generate new hyperparameters: atlas/lib/idds/atlas/processing/hyperparameteropt_bayesian.py
        b. The example code to generate requests: main/lib/idds/tests/hyperparameteropt_bayesian_test.py

    2. nevergrad: This is another predefined method in iDDS.

        a. The process to generate new hyperparameters: atlas/lib/idds/atlas/processing/hyperparameteropt_nevergrad.py
        b. The example code to generate requests: main/lib/idds/tests/hyperparameteropt_nevergrad_test.py

    3. container: Users can also define his own hyperparameter generator with container.

        b. Here is a docker example: main/lib/idds/tests/hyperparameteropt_docker_test.py


RESTful Service
----------------

1. To retrieve hyperparameters.

    client.get_hyperparameters(request_id, status=None, limit=None)

2. To register loss of a group of hyperparameters.

    client.update_hyperparameter(request_id, id, loss)

3. Example code: main/lib/idds/tests/hyperparameteropt_client_test.py


User-defied HPO Generator
--------------------------

For iDDS HPO, users can define their own HPO generator through docker. When iDDS calls these generators, iDDS will provide these parameters:

    a. '%MAX_POINTS': max number of groups of hyperparameters. iDDS will not stop generating new hyperparameters until it receives an output []. So the generator needs to take care of the max number of points.
    b. '%NUM_POINTS': The number of groups of hyperparameters per generation or per iteration. The default is 10. Users can also update it in the request with 'num_points_per_generation' in the request_metadata.
    c. '%IN': Every time when iDDS calls the generator, iDDS will generate a json file 'idds_input.json' in the current working directory which includes all points(one point is a group of hyperparameter) with corresponding loss or None(if loss is not available). When using docker, users need to mount the current directory and tell the generator the file path in container.
    d. '%OUT': The generator needs to create a an output json file which includes all new points. iDDS will read this file to parse all points. The 'OUT' file name is defined in the 'output_json' in the request_metadata.


Communication between the Pilot and User-defined HPO Training Program/Container
-------------------------------------------------------------------------------------

The pilot and user-defined HPO training program/container communicate with each other using the following files
in the current directory.
Their file names can be defined as HPO task parameters.

Input for HPO Training Program/Container
*****************************************
The pilot places two json files before running HPO training program/container.
One file contains a list of file names in the training dataset.
If those files are directly read from the storage the file contains a list of full paths.
The other file contains a set of hyperparameter to be evaluated.

Output from HPO Training Program/Container
***********************************************
HPO training program/container evaluates the hyperparameter set and produces one json file
which contains a dictionary with the following key-values: ``status``: ``integer`` (0: OK, others: Not Good),
``loss``: ``float``, ``message``: ``string`` (optional). It is possible to produce another json file to report
job metadata to PanDA which can contain an arbitrary dictionary.
