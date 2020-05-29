HyperParameterOptimization(HPO)
===============================

HPO is a usecase of iDDS. The purpose of iDDS HPO is to use iDDS to generate hyperparameters for machine learning and trigger production system to automatically process the machine learning training with provided hyperparameters.

iDDS HPO workflow
^^^^^^^^^^^^^^^^^

0. The user prepares two container images. One for optimization and sampling (Steering container), and the other for training (Evaluation container). He/she can use pre-defined methods if they meet his/her requirements.
1. The user submit a HPO task to JEDI.
2. JEDI submits a HPO request to iDDS.
3. iDDS generates multiple sets of hyperparameters (hyperparameter points) in the search space using the Steering container or pre-defined methods, and stores them in iDDS database. Each hyperparameter point has a serial number and corresponds to a single machine learning training job.
4. Serial numbers are sent to JEDI through ActiveMQ.
5. JEDI generates PanDA jobs and dispatches them to pilots running on compute resources.
6. Each PanDA job fetches a serial number from PanDA and converts it to the corresponding hyperparameter point by checking with iDDS.
7. Once the hyperparameter point is evaluated using the Evaluation container, the loss is registered to iDDS.
8. The PanDA job fetches another serial number and evaluates the corresponding hyperparameter point if wall-time is still available.
9. When the number of unprocessed hyperparameter points goes below a threshold, iDDS automatically generates new hyperparameter points. This step will continue until:

    a. The total number of hyperparameter points reaches max_points. This parameter can be defined in the request.
    b. The Steering container / pre-defined method fails or returns [].

10. When all hyperparameter points are evaluated, iDDS sends a message to JEDI to terminate the HPO task.


Optimization and Sampling
--------------------------

Currently iDDS support several different ways to generate hyperparameter points.
    1. bayesian: This is a pre-defied method in iDDS.

        a. The process to generate new hyperparameters: atlas/lib/idds/atlas/processing/hyperparameteropt_bayesian.py
        b. The example code to generate requests: main/lib/idds/tests/hyperparameteropt_bayesian_test.py

    2. nevergrad: This is another pre-defined method in iDDS.

        a. The process to generate new hyperparameters: atlas/lib/idds/atlas/processing/hyperparameteropt_nevergrad.py
        b. The example code to generate requests: main/lib/idds/tests/hyperparameteropt_nevergrad_test.py

    3. Steering container: Users can also provide their own container images to generate hyperparameter points. See `User-defied Steering Containers`_ for the details.

        b. Here is a docker example: main/lib/idds/tests/hyperparameteropt_docker_test.py


RESTful Service
----------------

1. To retrieve hyperparameters.

    client.get_hyperparameters(request_id, status=None, limit=None)

2. To register loss of a group of hyperparameters.

    client.update_hyperparameter(request_id, id, loss)

3. Example code: main/lib/idds/tests/hyperparameteropt_client_test.py



User-defied Steering Containers
--------------------------------

Users can provide their own container images to generate hyperparameter points using
the ask-and-tell pattern. Note that users need to use HPO packages such as skopt and
nevergrad which support the ask-and-tell pattern when making Steering containers.
Users can also provide execution strings to specify what are executed in containers.
Each execution string needs to contain the following placeholders like ``... --input=%IN ...``.

%MAX_POINTS
  The max number of hyperparameter points to be evaluated in the entire search. iDDS will not stop generating new hyperparameter points until it receives an empty list []. So the container needs to return [] if enough hyperparameter points are generated.

%NUM_POINTS
   The number of hyperparameter points to be generated in this call. The default is 10. It can be changed by setting 'num_points_per_generation' in the request_metadata.

%IN
   The name of input file which iDDS places in the current directory every time it calls the container. The file contains a json-formatted list of all hyperparameter points, which have been generated so far, with corresponding loss or None (if it is not yet evaluated).

%OUT
   The name of output file which the container creates in the current directory. The file contains a json-formatted list of new hyperparameter points.

When iDDS runs Steering containers, iDDS will replace %XYZ with actual parameters.
Input and output are done through json files in the current directly ($PWD) so that
the directory needs to be mounted.

Here is one example for the input (main/lib/idds/tests/idds_input.json). It is a json dump of
``{"points": [[{hyperparameter_point_1}, loss_or_None], ..., [{hyperparameter_point_N}, loss_or_None]], "opt_space": <opt space>}``.
``{hyperparameter_point}`` is a dictionary representing a single hyperparameter point.
The keys of the dictionary can be arbitrary strings and correspond to the axes of the search space.
For example, if there is a two dimensional search space with two hyperparameters, 'epochs' and 'batch_size',
the dictionary could be something like ``{'epochs': blah_1, 'batch_size': blah_2}``.
``points`` includes all hyperparameter points, which have been generated so far, whether or not they have been evaluated.
If a hyperparameter point is not yet evaluated, the ``loss_or_None`` will be None.
``opt_space`` is a copy of the content from your request. If in your request ``opt_space`` is not defined,
``opt_space`` will be None.

The output is a json dump of ``[{new_hyperparameter_point_1}, , ..., [{new_hyperparameter_point_N}]``.
``{new_hyperparameter_point}`` is a dictionary representing a new hyperparameter point.
The format of the dictionary is the same as the one in the input.

Basically what the Steering container needs to do is as follows:

1. Json-load %IN and update the optimizer with all hyperparameter points using the tell method.
2. Generate min(%NUM_POINTS, %MAX_POINTS-<the number of hyperparameter points generated so far>) new hyperparameter points using the ask method, and json-dump them to %OUT.

How to test the Steering container
************************************
Here is one example. Users can update the request part and test their docker locally.



Communication between the Pilot and User-defined Evaluation Container
-----------------------------------------------------------------------

The pilot and user-defined Evaluation container communicate with each other using the following files
in the current directory ($PWD), so that the directory needs to be mounted.
Their filenames can be defined in HPO task parameters.

Input for Evaluation Container
*****************************************
The pilot places two json files before running the Evaluation container.
One file contains a json-formatted list of all filenames in the training dataset,
i.e., it is a json-dump of ``[training_data_filename_1, training_data_filename_2, ..., training_data_filename_N]``.
If training data files need to be directly read from the storage the file contains a json-formatted list of full paths
to training data files.
The other file contains a single hyperparameter point to be evaluated.
A hyperparameter point is represented as a dictionary and the format of the dictionary follows
what the Steering container generated.
For example, if the Steering container generates a hyperparameter point like
``{'epochs': blah_1, 'batch_size': blah_2}``, the file will be a json-dump of
``{'epochs': blah_1, 'batch_size': blah_2}``.


Output from Evaluation Container
***********************************************
The Evaluation container evaluates the hyperparameter point and produces one json file.
The file contains a dictionary with the following key-values: ``status``: ``integer`` (0: OK, others: Not Good),
``loss``: ``float``, ``message``: ``string`` (optional). It is possible to produce another json file to report
job metadata to PanDA. It is a json-dump of an arbitrary dictionary, but the size must be less than 1MB.
It is also possible to produce a tarball to preserve training metrics. The tarball is uploaded to the storage
so that the size can be larger.
