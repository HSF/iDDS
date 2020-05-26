Active Learning(AL)
===================

Active Learning is an usecase of iDDS. The purpose of iDDS AL is to use iDDS to run some 'active learning' process to tell production system whether to continue some process.

iDDS AL  workflow
^^^^^^^^^^^^^^^^^

1. User creates an AL request with executable 'AL' process.
2. iDDS runs the 'AL' process which should generates an output json file.
3. iDDS parses the output json file and sends the output content to consumers.


The AL process
--------------

1. The AL process is an executable which runs in iDDS. It can be some executable shipped in a sandbox(through https) or some other extensions with new plugins.
2. The AL process should create an output json file. The json file name is defined in the 'output_json' in the request_metadata. Below is one example of the request.

    main/lib/idds/tests/activelearning_test.py

3. iDDS parses the output json file and sends the output content to consumers through ActiveMQ.
4. The consumers can define how to parse the outputs. For example, the consumer can stop further processing when the output contents is None or [].
